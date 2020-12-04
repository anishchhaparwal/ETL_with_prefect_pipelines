import os
from git import Repo
import datetime
import pandas as pd
from PIL import Image

from ETL_utils import filepath, purge_folder, Progress

from prefect import Flow, task, Parameter, unmapped
from prefect.environments import LocalEnvironment
from prefect.schedules import IntervalSchedule


@task(
    cache_for=datetime.timedelta(minutes=8), log_stdout=True, max_retries=2, retry_delay=datetime.timedelta(seconds=30)
)
def clone(dirpath, git_url):
    """Purges dirpath and clones git_url into dirpath

    Args:
        dirpath ([type]): path where git repo is to be cloned
        git_url ([type]): url of git repo
    """
    purge_folder(dirpath)
    Repo.clone_from(git_url, dirpath, progress=Progress())


@task
def filter_data(metadata_path, img_folder, results_folder):
    """Filters data for which images are present and above size 1023.

    Args:
        metadata_path ([str]): path where metadata.csv is present.
        img_folder ([str]): path where x-ray images are present.
        results_folder ([str]): path where final results are to be saved.

    Returns:
        [list]: list of image_paths
    """
    df = pd.read_csv(metadata_path)

    imgs_path = [file for file in os.listdir(img_folder)]
    df = df[df["filename"].isin(imgs_path)]

    for item in df["filename"]:
        df.loc[df["filename"] == item, "height"], df.loc[df["filename"] == item, "width"] = Image.open(
            os.path.join(img_folder, item)
        ).size

    df_included = df[(df.height > 1023) & (df.width > 1023)]
    df_included["fullfilepath"] = str(img_folder) + "/" + df["filename"].astype(str)

    purge_folder(results_folder)
    df_included.to_csv(os.path.join(results_folder, "included_csv_metadata.csv"))

    return df_included["fullfilepath"].tolist()


@task
def resize(image_path, results_folder):
    """resizing images to 224x224 and saving at desired location.

    Args:
        image_path ([str]): filepath of image to be ressized.
        results_folder ([str]): [description]
    """
    img = Image.open(image_path)
    img = img.resize((224, 224), Image.ANTIALIAS)
    img.save(os.path.join(results_folder, image_path))


schedule = IntervalSchedule(start_date=datetime.datetime.utcnow(), interval=datetime.timedelta(minutes=5))

if __name__ == "__main__":
    with Flow("ETL", schedule) as flow:

        # Input Parameters
        git_url = Parameter("git_url", default="https://github.com/ieee8023/covid-chestxray-dataset")
        input_data = filepath("", "data")
        metadata_path = Parameter("metadeta_path_file", default=filepath("data", "metadata.csv"))
        img_folder = Parameter("images_folder", default=filepath("data", "images"))
        results_folder = Parameter("result_folder", default=filepath("data", "results"))

        # Extraction
        clone_state = clone(input_data, git_url)

        # Transformation
        image_filepath = filter_data(metadata_path, img_folder, results_folder, upstream_tasks=[clone_state])

        # Load
        resize.map(file_metadata=image_filepath, results_folder=unmapped(results_folder))

    flow.run()
    # flow.environment = LocalEnvironment(labels="myETL")
    # flow.register(project_name="ETL_Project")