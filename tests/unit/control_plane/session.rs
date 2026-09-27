use super::*;
use serde_json::json;

#[test]
fn project_sessions_allow_all_actors_and_only_published_methods() -> Result<()> {
    let session: ActorSession = serde_json::from_value(
        json!({"iss":"runtime", "aud":"sessions", "sub":"credential", "jti":"session",
        "projectId":"project", "scope":"actor:session", "iat":1000,"nbf":1000,"exp":1060}),
    )?;
    session.validate(1000)?;
    let actor = ActorKey {
        project_id: "project".into(),
        actor_name: "Counter".into(),
        actor_id: "one".into(),
    };
    for (actor_name, actor_id) in [("Counter", "one"), ("Counter", "two"), ("Room", "one")] {
        let grant = session.clone().invocation(
            &ActorKey {
                actor_name: actor_name.into(),
                actor_id: actor_id.into(),
                ..actor.clone()
            },
            vec!["read".into(), "write".into()],
        )?;
        assert_eq!(grant.methods, vec!["read", "write"]);
        assert_eq!(grant.subject, session.subject);
        assert_eq!(grant.grant_id, session.jti);
        assert_eq!(grant.expires_at, session.expires_at);
    }
    assert!(session.clone().invocation(&actor, vec![]).is_err());
    assert!(
        session
            .clone()
            .invocation(
                &ActorKey {
                    project_id: "other".into(),
                    ..actor
                },
                vec!["read".into()]
            )
            .is_err()
    );
    assert!(session.validate(1060).is_err());
    Ok(())
}
