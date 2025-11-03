import requests
from requests import Session


def authenticate_user(
    session: Session, email: str, password: str, base_url_rest: str
) -> bool:
    """
    Authenticate a user with DSpace API.

    Args:
        session: requests Session object.
        email: User email.
        password: User password.
        base_url_rest: Base URL for DSpace REST API.

    Raises:
        SystemExit: If authentication fails.
    """
    try:
        login_url = f"{base_url_rest}/login"
        credentials = {"email": email, "password": password}
        response = session.post(login_url, data=credentials)
        response.raise_for_status()
        print("Successful authentication.")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Authentication error: {e}")
        return False
