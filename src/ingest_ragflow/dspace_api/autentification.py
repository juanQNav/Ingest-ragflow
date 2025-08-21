import requests


def authenticate_user(session, email, password, base_url_rest):
    try:
        login_url = f"{base_url_rest}/login"
        credentials = {"email": email, "password": password}
        response = session.post(login_url, data=credentials)
        response.raise_for_status()
        print("Successful authentication.")
    except requests.exceptions.RequestException as e:
        print(f"Authentication error: {e}")
        exit()
