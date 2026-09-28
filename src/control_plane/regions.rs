pub(super) const ALL: &[&str] = &[
    "canada",
    "north-america-east",
    "north-america-central",
    "north-america-south",
    "north-america-west",
    "europe-west",
    "asia-southeast",
];

use anyhow::{Result, bail};

pub(super) fn storage_region(region: &str) -> Result<&str> {
    let location = match region.rsplit_once('-') {
        Some((base, "a" | "b" | "c" | "d" | "f"))
            if matches!(
                base,
                "us-east1"
                    | "northamerica-northeast1"
                    | "northamerica-northeast2"
                    | "us-east4"
                    | "us-east5"
                    | "us-central1"
                    | "us-south1"
                    | "us-west1"
                    | "us-west2"
                    | "us-west3"
                    | "us-west4"
                    | "europe-west1"
                    | "europe-west3"
                    | "europe-west4"
                    | "asia-southeast1"
                    | "asia-southeast2"
            ) =>
        {
            base
        }
        _ => region,
    };
    match location {
        "canada"
        | "north-america-east"
        | "north-america-central"
        | "north-america-south"
        | "north-america-west"
        | "europe-west"
        | "asia-southeast" => Ok(region),
        "northamerica-northeast1" | "northamerica-northeast2" => Ok("canada"),
        "us-east-1" | "us-east-2" | "us-east1" | "us-east4" | "us-east5" | "us-ashburn-1"
        | "eastus" | "eastus2" => Ok("north-america-east"),
        "us-central1" | "us-chicago-1" | "centralus" | "northcentralus" => {
            Ok("north-america-central")
        }
        "us-south1" | "southcentralus" => Ok("north-america-south"),
        "us-west-1" | "us-west-2" | "us-west1" | "us-west2" | "us-west3" | "us-west4"
        | "us-phoenix-1" | "us-sanjose-1" | "westus" | "westus2" | "westus3" | "westcentralus" => {
            Ok("north-america-west")
        }
        "eu-west-1" | "eu-west-3" | "eu-central-1" | "europe-west1" | "europe-west3"
        | "europe-west4" | "eu-frankfurt-1" | "eu-paris-1" | "westeurope" => Ok("europe-west"),
        "ap-southeast-1" | "asia-southeast1" | "asia-southeast2" | "ap-singapore-1"
        | "southeastasia" => Ok("asia-southeast"),
        _ => bail!("workflow region {region:?} has no storage-region mapping"),
    }
}
