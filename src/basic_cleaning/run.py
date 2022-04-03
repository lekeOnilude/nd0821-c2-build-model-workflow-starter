#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import os
import tempfile
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info(f"Downloading file {args.input_artifact}")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    with tempfile.TemporaryDirectory() as tmpdir:
        df = pd.read_csv(artifact_local_path, low_memory=False)

        logger.info(f"Remove datapoint below {args.min_price} and above {args.max_price}")
        idx = df['price'].between(int(args.min_price), int(args.max_price))
        df = df[idx].copy()
        
        idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
        df = df[idx].copy()
        # Convert last_review to datetime
        df['last_review'] = pd.to_datetime(df['last_review'])

        output_path = os.path.join(tmpdir, args.output_artifact)
        
        logger.info("save output file to csv")
        df.to_csv(output_path, index=False)

        logger.info(f"Add artifact {args.output_artifact} to wandb")
        artifact = wandb.Artifact(
            args.output_artifact,
            type=args.output_type,
            description=args.output_description,
        )

        artifact.add_file(output_path)
        run.log_artifact(artifact)

        # This waits for the artifact to be uploaded to W&B. If you
        artifact.wait()




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="name of the  input artifact with the version",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help= "Output artifact name",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help= "Output type",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help= "Clean dataset",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=str,
        help= "Min proce range to consider",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=str,
        help= "Max price range to consider",
        required=True
    )


    args = parser.parse_args()

    go(args)
