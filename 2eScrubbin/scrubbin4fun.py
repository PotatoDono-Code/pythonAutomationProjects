from git import Repo
import os

rd = "2e Datasets"

if not os.path.exists(rd):
    repo = Repo.clone_from("https://github.com/foundryvtt/pf2e.git", rd, no_checkout = True, branch="release")
else:
    repo = Repo(rd)

