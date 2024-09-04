"""
An ETL for reading from the Hugging Face API spaces endpoint, doing some minor transformations and calculations on the results and writing everything out to a Parquet file. 

The Parquet file is then pushed to a dataset on the Hugging Face Hub where it is showed in a Space, but can also be used by anyone in the community for futher analysis.
"""

from huggingface_hub import list_spaces, space_info
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def get_spaces():
    """
    A generator that returns all spaces from the Hugging Face API
    """
    for space in iter(list_spaces(full=True)):
        yield space


def get_space_info(space_id):
    """
    Get the metadata of a space
    """
    return space_info(space_id, files_metadata=True)


def load_into_dataframe(space, space_list):
    """
    Add a space to the list of spaces
    """
    # try to get the list of custom domains for a spce
    try:
        custom_domains = [
            domain["domain"]
            for domain in space.runtime.raw["domains"]
            if domain["isCustom"]
        ]
    except KeyError:
        custom_domains = None

    # try to get the length of a readme file for a space
    try:
        readme_size = [f.size for f in space.siblings if f.rfilename == "README.md"][0]
    except IndexError:
        readme_size = None

    # try to get the card data unless it's none then set all card data to none
    try:
        python_version = str(space.card_data.get("python_version", None))
        space_license = space.card_data.get("license", None)
        duplicated_from = space.card_data.get("duplicated_from", None)
        models = space.card_data.get("models", None)
        datasets = space.card_data.get("datasets", None)
        emoji = space.card_data.get("emoji", None)
        colorFrom = space.card_data.get("colorFrom", None)
        colorTo = space.card_data.get("colorTo", None)
        pinned = space.card_data.get("pinned", None)
        print(
            python_version, license, models, datasets, emoji, colorFrom, colorTo, pinned
        )
    except AttributeError:
        python_version = None
        space_license = None
        duplicated_from = None
        models = None
        datasets = None
        emoji = None
        colorFrom = None
        colorTo = None
        pinned = None

    space_list.append(
        {
            "id": space.id,
            "author": space.author,
            "created_at": space.created_at,
            "last_modified": space.last_modified,
            "subdomain": space.subdomain,
            "host": space.host,
            "likes": space.likes,
            "sdk": space.sdk,
            "tags": space.tags,
            "readme_size": readme_size,
            "python_version": python_version,
            "license": space_license,
            "duplicated_from": duplicated_from,
            "models": models,
            "datasets": datasets,
            "emoji": emoji,
            "colorFrom": colorFrom,
            "colorTo": colorTo,
            "pinned": pinned,
            "stage": space.runtime.stage,
            "hardware": space.runtime.hardware,
            "devMode": space.runtime.raw.get("devMode", None),
            "custom_domains": custom_domains,
        },
    )
    return space_list


def push_to_hub(df):
    """
    Push the Parquet file to the Hugging Face Hub as a dataset
    """
    pass


def build():
    spaces = get_spaces()
    df = pd.DataFrame(
        columns=[
            "id",
            "author",
            "created_at",
            "last_modified",
            "subdomain",
            "host",
            "likes",
            "sdk",
            "tags",
            "readme_size",
            "python_version",
            "license",
            "duplicated_from",
            "models",
            "datasets",
            "emoji",
            "colorFrom",
            "colorTo",
            "pinned",
            "stage",
            "hardware",
            "devMode",
            "custom_domains",
        ]
    )
    space_list = []
    for i in range(500):
        next_space = next(spaces)
        curr_space = get_space_info(next_space.id)
        space_list = load_into_dataframe(curr_space, space_list)

    # load the list into the dataframe
    df = pd.DataFrame(space_list)
    # print the first 20 models where the model is not None
    # print(df[df["models"].notnull()].head(20))
    # write the dataframe to a parquet file
    df.to_parquet("spaces.parquet")


if __name__ == "__main__":
    build()
