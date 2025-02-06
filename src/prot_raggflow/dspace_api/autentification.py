def authenticate_user(session, email, password, base_url_rest):
    login_url = f"{base_url_rest}/login"
    credentials = {"email": email, "password": password}
    response = session.post(login_url, data=credentials)
    
    if response.status_code == 200:
        print("Successful authentication.")
    else:
        print("Authentication error:", response.text)
        exit()
