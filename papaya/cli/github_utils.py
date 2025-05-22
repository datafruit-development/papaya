from typing import List, Union, Optional
from github import Github
from github.ContentFile import ContentFile
from github.Repository import Repository
import os
from dotenv import load_dotenv

load_dotenv()

def get_github_client(token: Optional[str] = None) -> Github:
    """
    Creates and returns a GitHub client instance.
    
    Args:
        token: GitHub Personal Access Token. If None, uses GH_APP_TOKEN env var.
        
    Returns:
        An authenticated Github client instance.
    """
    token = token or os.getenv("GH_APP_TOKEN")
    if not token:
        raise ValueError("GitHub token must be provided either directly or via GH_APP_TOKEN environment variable")
    return Github(token)

def get_repo_contents(owner: str, repo: str, path: str = "", token: Optional[str] = None) -> List[ContentFile]:
    """
    Gets the contents of a directory or a specific file in a repository.

    Args:
        owner: The owner of the repository.
        repo: The name of the repository.
        path: The path to the directory or file. If empty, gets root contents.
        token: Optional GitHub Personal Access Token. If None, uses GH_APP_TOKEN env var.

    Returns:
        A list of ContentFile objects representing the items in the directory.
        For files, returns a list with a single ContentFile.
    """


    gh = get_github_client(token)
    repository = gh.get_repo(f"{owner}/{repo}")
    
    try:
        contents = repository.get_contents(path.lstrip('/') if path else '')
        # If it's a single file, wrap it in a list for consistent return type
        if isinstance(contents, ContentFile):
            return [contents]
        return contents
    except Exception as e:
        raise Exception(f"Failed to get contents for {owner}/{repo}/{path}: {str(e)}")

def get_file_contents(owner: str, repo: str, path: str, token: Optional[str] = None) -> ContentFile:
    """
    Gets the contents of a specific file in a repository.

    Args:
        owner: The owner of the repository.
        repo: The name of the repository.
        path: The path to the file. Must not be empty.
        token: Optional GitHub Personal Access Token. If None, uses GH_APP_TOKEN env var.

    Returns:
        A ContentFile object representing the file contents.
        You can access the decoded content using .decoded_content
        and other metadata like .name, .path, .sha, etc.
    """

    if not path:
        raise ValueError("Path cannot be empty for get_file_contents.")

    gh = get_github_client(token)
    repository = gh.get_repo(f"{owner}/{repo}")
    
    try:
        return repository.get_contents(path.lstrip('/'))
    except Exception as e:
        raise Exception(f"Failed to get file contents for {owner}/{repo}/{path}: {str(e)}")


def make_code_change(owner: str, repo: str, new_content: dict, branch_name: str = None, commit_message: str = None, pr_title: str = None, pr_body: str = None):
    """
    Makes a code change to a file in a repository on a Lychee-specific feature branch, and makes a PR.

    Args:
        owner: The owner of the repository.
        repo: The name of the repository.
        new_content: A dictionary containing the new content for the file.
                    The keys should be the path to the file, and the values should be the new content.
        branch_name: Optional name for the feature branch. If None, generates one based on timestamp.
        commit_message: Optional commit message. If None, uses a default message.
        pr_title: Optional PR title. If None, uses a default title.
        pr_body: Optional PR body. If None, uses a default body.

    Returns:
        The URL of the created pull request.
    """
    gh = get_github_client()
    repository = gh.get_repo(f"{owner}/{repo}")

    # Get the default branch (usually main or master)
    default_branch = repository.default_branch
    base_branch = repository.get_branch(default_branch)

    # Generate branch name if not provided
    if not branch_name:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        branch_name = f"lychee/fix_{timestamp}"

    # Create new branch from default branch
    try:
        ref = f"refs/heads/{branch_name}"
        repository.create_git_ref(ref=ref, sha=base_branch.commit.sha)
    except Exception as e:
        raise Exception(f"Failed to create branch {branch_name}: {str(e)}")

    # Make changes to files
    try:
        for file_path, content in new_content.items():
            try:
                # Try to get existing file
                file = repository.get_contents(file_path, ref=branch_name)
                repository.update_file(
                    path=file_path,
                    message=commit_message or f"Update {file_path}",
                    content=content,
                    sha=file.sha,
                    branch=branch_name
                )
            except Exception:
                # File doesn't exist, create it
                repository.create_file(
                    path=file_path,
                    message=commit_message or f"Create {file_path}",
                    content=content,
                    branch=branch_name
                )
    except Exception as e:
        # Clean up branch if file operations fail
        try:
            ref = repository.get_git_ref(f"heads/{branch_name}")
            ref.delete()
        except:
            pass
        raise Exception(f"Failed to update files: {str(e)}")

    # Create pull request
    try:
        pr = repository.create_pull(
            title=pr_title or f"Lychee: Updates from {branch_name}",
            body=pr_body or "Automated pull request created by Lychee",
            head=branch_name,
            base=default_branch
        )
        return pr.html_url
    except Exception as e:
        # Clean up branch if PR creation fails
        try:
            ref = repository.get_git_ref(f"heads/{branch_name}")
            ref.delete()
        except:
            pass
        raise Exception(f"Failed to create pull request: {str(e)}")

    

if __name__ == "__main__":
    print(get_repo_contents("lychee-development", "spark_debugger_demo"))