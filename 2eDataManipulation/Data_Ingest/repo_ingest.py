from git import Repo
import os

# -- Target the directory for the 2e Data Repo
rd = "2e Datasets"

# -- Either clone from scratch or update. Only deal with files listed in the 'packs' subfolder
if not os.path.exists(rd):
    repo = Repo.clone_from("https://github.com/foundryvtt/pf2e.git", rd, no_checkout = True, branch="release")
    print(f"Creating directory from Foundry VTT Repo")
else:
    repo = Repo(rd)
    print(f"Updating directory from Foundry VTT Repo")


git_cmd = repo.git
git_cmd.sparse_checkout('init', '--cone')
git_cmd.sparse_checkout('set', 'packs')
git_cmd.checkout('release')

print(f"File directory complete and ready for use")