import streamlit as st
import requests
from datetime import datetime

# Configuration de la page
st.set_page_config(
    page_title="Docker Hub",
    page_icon="🐳",
    layout="wide",
    initial_sidebar_state="expanded",
)

NAMESPACE = "robdeopaproject"
SERVER_DOCKERHUB_URL = f"https://hub.docker.com/u/{NAMESPACE}"

def get_repositories():
    url = f"https://hub.docker.com/v2/namespaces/{NAMESPACE}/repositories?page_size=100"
    return requests.get(url).json()["results"]

def get_tags(repo_name):
    url = f"https://hub.docker.com/v2/namespaces/{NAMESPACE}/repositories/{repo_name}/tags?page_size=5"
    return requests.get(url).json()["results"]

def display_repo(repo):

    with st.container(border=True):

        c1, c2 = st.columns([1, 5])

        with c1:
            st.image(
                "https://www.docker.com/wp-content/uploads/2022/03/Moby-logo.png",
                width=48,
            )

        with c2:

            st.markdown(f"### {repo['name']}")

            if repo["description"]:
                st.caption(repo["description"])

        m1, m2, m3 = st.columns(3)

        m1.metric("⬇ Pulls", f"{repo['pull_count']:,}")
        m2.metric("⭐ Stars", repo["star_count"])
        m3.metric(
            "🕒",
            datetime.fromisoformat(
                repo["last_updated"].replace("Z", "+00:00")
            ).strftime("%d/%m/%y"),
        )

        tags = get_tags(repo["name"])

        if tags:
            badges = " ".join(
                [
                    f":blue-badge[{tag['name']}]"
                    for tag in tags
                ]
            )
            st.markdown(badges)

        st.link_button(
            "Voir sur Docker Hub",
            f"https://hub.docker.com/r/{NAMESPACE}/{repo['name']}",
            use_container_width=True,
        )

@st.fragment(run_every="5s")
def diplay_page():
    repos = get_repositories()

    col1, col2 = st.columns(2)

    for i, repo in enumerate(repos):
        with col1 if i % 2 == 0 else col2:
            display_repo(repo)

def main():
    st.header("Docker Hub", divider="gray")

    st.info(f"URL de base : **{SERVER_DOCKERHUB_URL}**")

    diplay_page()


if __name__ == "__main__":
    main()
