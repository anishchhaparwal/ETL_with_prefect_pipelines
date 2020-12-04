import os
import inspect
import shutil
import git
import stat


def filepath(Folder, File):
    """Finds current working directory and appends to path

    Args:
        Folder ([str]): name of folder to be appended to path
        File ([str]): name of file to be appended to path

    Returns:
        [str]: appened filepath
    """
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(inspect.stack()[0][1])))
    read_file = os.path.join(cwd, Folder, File)
    return read_file


def purge_folder(dirpath):
    """Deletes all files along with folder if it exists and creates new folder.

    Args:
        dirpath ([str]): folder path to be purged.
    """
    if os.path.exists(dirpath):
        for root, dirs, files in os.walk(dirpath):
            for dir in dirs:
                os.chmod(os.path.join(root, dir), stat.S_IRWXU)
            for file in files:
                os.chmod(os.path.join(root, file), stat.S_IRWXU)
        shutil.rmtree(dirpath)
    os.makedirs(dirpath)


class Progress(git.remote.RemoteProgress):
    def update(self, op_code, cur_count, max_count=None, message=""):
        print(self._cur_line)
