use serde_json::json;

use super::*;
use crate::actor::validate_socket_effects;

#[tokio::test]
async fn admission_enforces_connection_limit_and_reopens_after_disconnect() {
    let registry = SocketRegistry::default();
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Room".into(),
        actor_id: "large".into(),
    };
    let (sender, _receiver) = socket_channel();
    for index in 0..32768 {
        assert!(
            registry
                .insert(
                    &actor,
                    ActorSocketConnection {
                        id: index.to_string(),
                        metadata: Value::Null,
                        tags: vec![]
                    },
                    sender.clone(),
                )
                .await,
            "connection {index}"
        );
    }
    let connection = ActorSocketConnection {
        id: "next".into(),
        metadata: Value::Null,
        tags: vec![],
    };
    assert!(
        !registry
            .insert(&actor, connection.clone(), sender.clone())
            .await
    );
    registry.remove(&actor, "0").await;
    assert!(registry.insert(&actor, connection, sender).await);
}

#[test]
fn metadata_and_tags_enforce_size_limits() {
    assert!(crate::actor::validate_socket_metadata(&json!("x".repeat(16382))).is_ok());
    assert!(crate::actor::validate_socket_metadata(&json!("x".repeat(16383))).is_err());
    for (count, valid) in [(10, true), (11, false)] {
        let effect = ActorSocketEffect::SetTags {
            connection_id: "socket".into(),
            tags: (0..count)
                .map(|i| format!("{i:02}{}", "x".repeat(254)))
                .collect(),
        };
        assert_eq!(validate_socket_effects(&[effect]).is_ok(), valid);
    }
}

#[tokio::test]
async fn message_preparation_copies_only_the_originating_connection() {
    let registry = SocketRegistry::default();
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Room".into(),
        actor_id: "large".into(),
    };
    for id in ["sender", "other"] {
        let (outbound, _receiver) = socket_channel();
        assert!(
            registry
                .insert(
                    &actor,
                    ActorSocketConnection {
                        id: id.into(),
                        metadata: json!({"id":id}),
                        tags: vec![]
                    },
                    outbound,
                )
                .await
        );
        registry.activate(&actor, id).await;
    }
    let (_, connections) = registry
        .prepare_event(
            &actor,
            ActorSocketEvent::Message {
                connection_id: "sender".into(),
                message: ActorSocketMessage::Text {
                    data: "null".into(),
                },
            },
        )
        .await;
    assert_eq!(connections.len(), 1);
    assert_eq!(connections[0].id, "sender");
    assert_eq!(registry.connections_with_tag(&actor, None).await.len(), 2);
}

#[tokio::test]
async fn broadcast_matches_all_or_any_tags_and_preserves_exclusions() {
    let registry = SocketRegistry::default();
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "Files".into(),
        actor_id: "files".into(),
    };
    let mut receivers = Vec::new();
    for (id, tags, open) in [
        ("a", vec!["file:a"], true),
        ("b", vec!["file:b"], true),
        ("both", vec!["file:a", "file:b", "file:c"], true),
        ("neither", vec!["file:c"], true),
        ("excluded", vec!["file:a", "file:b"], true),
        ("connecting", vec!["file:a", "file:b"], false),
    ] {
        let (outbound, receiver) = socket_channel();
        assert!(
            registry
                .insert(
                    &actor,
                    ActorSocketConnection {
                        id: id.into(),
                        metadata: json!({}),
                        tags: Vec::new(),
                    },
                    outbound,
                )
                .await
        );
        if open {
            registry.activate(&actor, id).await;
        }
        registry
            .apply(
                &actor,
                vec![ActorSocketEffect::SetTags {
                    connection_id: id.into(),
                    tags: tags.into_iter().map(String::from).collect(),
                }],
            )
            .await;
        receivers.push((id, receiver));
    }
    for (mode, tags, expected) in [
        (None, vec!["file:a", "file:b"], vec!["both"]),
        (Some("all"), vec!["file:a", "file:b"], vec!["both"]),
        (
            Some("any"),
            vec!["file:a", "file:b"],
            vec!["a", "b", "both"],
        ),
        (Some("any"), vec!["file:missing"], vec![]),
        (Some("all"), vec![], vec!["a", "b", "both", "neither"]),
        (Some("any"), vec![], vec!["a", "b", "both", "neither"]),
    ] {
        let mut effect = json!({
            "type": "broadcast", "message": {"type": "text", "data": "ready"},
            "except_connection_ids": ["excluded"], "tags": tags,
        });
        if let Some(mode) = mode {
            effect["tag_match"] = json!(mode);
        }
        registry
            .apply(&actor, vec![serde_json::from_value(effect).unwrap()])
            .await;
        for (id, receiver) in &mut receivers {
            let delivered = receiver.try_recv().ok();
            assert_eq!(
                delivered.is_some(),
                expected.contains(id),
                "mode={mode:?}, socket={id}"
            );
            if let Some(message) = delivered {
                assert!(
                    matches!(message, OutboundMessage::Message(ActorSocketMessage::Text { data }) if data == "ready")
                );
            }
            assert!(receiver.try_recv().is_err(), "duplicate delivery to {id}");
        }
    }
}

#[test]
fn broadcast_rejects_invalid_tag_matching_mode() {
    assert!(
        serde_json::from_value::<ActorSocketEffect>(json!({
            "type": "broadcast", "message": {"type": "text", "data": "ready"},
            "except_connection_ids": [], "tags": [], "tag_match": "either",
        }))
        .is_err()
    );
}

#[test]
fn accepts_structured_public_state_effects_and_rejects_invalid_snapshots() -> anyhow::Result<()> {
    let effects: Vec<ActorSocketEffect> = serde_json::from_value(json!([
        {"type":"state_snapshot","connection_id":"socket","state":{"count":1}},
        {"type":"state_update","changes":{"count":2},"removed":["optional"]}
    ]))?;
    validate_socket_effects(&effects)?;
    let invalid: Vec<ActorSocketEffect> = serde_json::from_value(json!([
        {"type":"state_snapshot","connection_id":"socket","state":null}
    ]))?;
    assert!(validate_socket_effects(&invalid).is_err());
    Ok(())
}

#[test]
fn rejects_socket_effects_that_bypass_sdk_invariants() {
    let invalid = [
        ActorSocketEffect::Close {
            connection_id: "socket-1".into(),
            code: 1001,
            reason: String::new(),
        },
        ActorSocketEffect::SetTags {
            connection_id: "socket-1".into(),
            tags: vec!["x".repeat(257)],
        },
        ActorSocketEffect::SetMetadata {
            connection_id: "socket-1".into(),
            metadata: json!({ "data": "x".repeat(64 * 1024) }),
        },
        ActorSocketEffect::Send {
            connection_id: "socket-1".into(),
            message: ActorSocketMessage::Binary {
                data: "not base64".into(),
            },
        },
    ];

    for effect in invalid {
        assert!(crate::actor::validate_socket_effects(&[effect]).is_err());
    }
}

#[tokio::test]
async fn registry_retains_metadata_tags_and_outbound_messages() {
    let registry = SocketRegistry::default();
    let actor = ActorKey {
        project_id: "default".into(),
        actor_name: "ChatRoom".into(),
        actor_id: "room-1".into(),
    };
    let (outbound, mut messages) = socket_channel();
    assert!(
        registry
            .insert(
                &actor,
                ActorSocketConnection {
                    id: "socket-1".into(),
                    metadata: json!({ "userId": "user-1" }),
                    tags: Vec::new(),
                },
                outbound,
            )
            .await
    );
    registry.activate(&actor, "socket-1").await;

    registry
        .apply(
            &actor,
            vec![
                ActorSocketEffect::SetMetadata {
                    connection_id: "socket-1".into(),
                    metadata: json!({ "userId": "user-1", "ready": true }),
                },
                ActorSocketEffect::SetTags {
                    connection_id: "socket-1".into(),
                    tags: vec!["member".into()],
                },
                ActorSocketEffect::Send {
                    connection_id: "socket-1".into(),
                    message: ActorSocketMessage::Text {
                        data: "hello".into(),
                    },
                },
                ActorSocketEffect::Broadcast {
                    message: ActorSocketMessage::Text {
                        data: "everyone".into(),
                    },
                    except_connection_ids: Vec::new(),
                    tags: vec!["member".into()],
                    tag_match: ActorSocketTagMatch::All,
                },
            ],
        )
        .await;

    assert_eq!(
        registry.connections_with_tag(&actor, None).await,
        vec![ActorSocketConnection {
            id: "socket-1".into(),
            metadata: json!({ "userId": "user-1", "ready": true }),
            tags: vec!["member".into()],
        }]
    );
    assert!(matches!(
        messages.recv().await,
        Some(OutboundMessage::Message(ActorSocketMessage::Text { data })) if data == "hello"
    ));
    assert!(matches!(
        messages.recv().await,
        Some(OutboundMessage::Message(ActorSocketMessage::Text { data })) if data == "everyone"
    ));
}

#[tokio::test]
async fn connection_queries_count_active_sockets_and_filter_tags() {
    let registry = SocketRegistry::default();
    let actor = ActorKey {
        project_id: "p".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
    };
    let (sender, _receiver) = socket_channel();
    for (id, tags) in [("a", vec!["blue".into()]), ("b", vec!["red".into()])] {
        assert!(
            registry
                .insert(
                    &actor,
                    ActorSocketConnection {
                        id: id.into(),
                        metadata: Value::Null,
                        tags
                    },
                    sender.clone(),
                )
                .await
        );
    }
    assert_eq!(registry.count(&actor).await, 0);
    registry.activate(&actor, "a").await;
    assert_eq!(registry.count(&actor).await, 1);
    registry.activate(&actor, "a").await;
    assert_eq!(registry.count(&actor).await, 1);
    registry.activate(&actor, "b").await;
    assert_eq!(
        registry
            .connections_with_tag(&actor, Some("blue"))
            .await
            .iter()
            .map(|c| c.id.as_str())
            .collect::<Vec<_>>(),
        ["a"]
    );
    registry.remove(&actor, "a").await;
    assert_eq!(registry.count(&actor).await, 1);
}

#[tokio::test]
async fn automatic_responses_are_scoped_to_the_actor_and_can_be_cleared() {
    let registry = SocketRegistry::default();
    let actor = ActorKey {
        project_id: "p".into(),
        actor_name: "Room".into(),
        actor_id: "one".into(),
    };
    let other = ActorKey {
        actor_id: "other".into(),
        ..actor.clone()
    };
    registry
        .apply(
            &actor,
            vec![ActorSocketEffect::SetAutoResponse {
                request: Some("ping".into()),
                response: Some("pong".into()),
            }],
        )
        .await;
    assert_eq!(
        registry.auto_response(&actor, "ping").await.as_deref(),
        Some("pong")
    );
    assert_eq!(registry.auto_response(&other, "ping").await, None);
    assert_eq!(registry.auto_response(&actor, "other").await, None);
    registry
        .apply(
            &actor,
            vec![ActorSocketEffect::SetAutoResponse {
                request: None,
                response: None,
            }],
        )
        .await;
    assert_eq!(registry.auto_response(&actor, "ping").await, None);
}
