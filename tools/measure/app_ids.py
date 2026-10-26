"""Print the registered function apps: name, id, active layout and component names."""
import json
import urllib.request

API_URL = "http://localhost:8080/function_apps/"


def main():
    with urllib.request.urlopen(API_URL) as resp:
        apps = json.load(resp)
    for app in apps:
        name = app.get("name", "?")
        app_id = app.get("id", "?")
        layout = app.get("active_layout_key") or "-"
        components = ", ".join(c["name"] for c in app.get("components") or [])
        print(f"{name:40s} {app_id:28s} {layout:10s} {components}")


if __name__ == "__main__":
    main()