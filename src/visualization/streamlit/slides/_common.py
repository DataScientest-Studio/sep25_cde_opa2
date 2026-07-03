
from pathlib import Path
import base64
import html
import re
import streamlit as st

ASSETS_DIR = Path(__file__).resolve().parents[1] / "assets" / "slides"


def apply_slide_style():
    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 1.6rem;
                padding-bottom: 1.2rem;
                max-width: 1180px;
            }
            h1 {
                letter-spacing: -0.03em;
                font-size: 2.35rem !important;
                line-height: 1.12 !important;
                color: #0f172a !important;
                margin-top: 0.15rem !important;
                margin-bottom: 0.55rem !important;
            }
            .stHeading h1 span {
                color: #0369a1;
                font-size: 2.25rem;
                margin-bottom: -1rem;
            }
            .slide-subtitle {
                font-size: 1.75rem;
                line-height: 1.55;
                color: #334155;
                margin-bottom: 1rem;
                max-width: 900px;
            }
            .slide-lead {
                font-size: 1.05rem;
                line-height: 1.55;
                color: #334155;
                margin-bottom: 1rem;
                max-width: 900px;
            }
            .slide-card {
                background: #f8fafc;
                color: #1e293b;
                border: 1px solid #cbd5e1;
                border-radius: 16px;
                padding: 0.9rem 1rem;
                margin-bottom: 0.7rem;
                box-shadow: 0 2px 8px rgba(15, 23, 42, 0.05);
            }
            .slide-card strong {
                color: #0f172a;
                font-size: 1rem;
            }
            .slide-card code {
                background: #e2e8f0;
                color: #0f172a;
                padding: 0.08rem 0.3rem;
                border-radius: 6px;
            }
            .slide-note {
                color: #475569;
                font-size: 0.92rem;
                margin-top: 0.35rem;
            }
            .metric-row {
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 0.8rem;
                margin: 0.7rem 0 1rem 0;
            }
            .metric-box {
                border-radius: 16px;
                padding: 0.9rem 1rem;
                background: linear-gradient(135deg, #eff6ff, #f8fafc);
                border: 1px solid #bfdbfe;
                min-height: 108px;
                box-shadow: 0 2px 8px rgba(14, 165, 233, 0.06);
            }
            .metric-value {
                font-size: 1.55rem;
                font-weight: 800;
                color: #0f172a;
                margin-bottom: 0.2rem;
            }
            .metric-label {
                color: #334155;
                font-size: 0.93rem;
                line-height: 1.35;
            }
            .slide-image-toggle {
                display: none;
            }
            .slide-image-wrap {
                width: 100%;
                display: flex;
                justify-content: center;
                align-items: center;
                background: #ffffff;
                border: 1px solid #cbd5e1;
                border-radius: 16px;
                padding: 0.6rem;
                box-shadow: 0 2px 10px rgba(15, 23, 42, 0.06);
                overflow: hidden;
                cursor: zoom-in;
                position: relative;
                transition: border-color 160ms ease, box-shadow 160ms ease, transform 160ms ease;
            }
            .slide-image-wrap:hover {
                border-color: #38bdf8;
                box-shadow: 0 5px 18px rgba(14, 165, 233, 0.15);
                transform: translateY(-1px);
            }
            .slide-image-wrap::after {
                content: "Cliquer pour agrandir";
                position: absolute;
                right: 0.75rem;
                bottom: 0.75rem;
                background: rgba(15, 23, 42, 0.82);
                color: #ffffff;
                font-size: 0.78rem;
                font-weight: 650;
                padding: 0.35rem 0.55rem;
                border-radius: 999px;
                opacity: 0;
                transform: translateY(4px);
                transition: opacity 160ms ease, transform 160ms ease;
                pointer-events: none;
            }
            .slide-image-wrap:hover::after {
                opacity: 1;
                transform: translateY(0);
            }
            .slide-image {
                width: 100%;
                max-width: 540px;
                max-height: 340px;
                object-fit: contain;
                display: block;
                border-radius: 10px;
            }
            .slide-caption {
                color: #475569;
                font-size: 0.9rem;
                margin-top: 0.35rem;
                text-align: center;
            }
            .slide-modal {
                display: none;
                position: fixed;
                inset: 0;
                z-index: 999999;
                padding: 3.5vh 3.5vw;
                align-items: center;
                justify-content: center;
            }
            .slide-modal-backdrop {
                position: absolute;
                inset: 0;
                background: rgba(15, 23, 42, 0.78);
                backdrop-filter: blur(3px);
                cursor: zoom-out;
            }
            .slide-modal-content {
                position: relative;
                z-index: 1;
                width: min(96vw, 1400px);
                max-height: 92vh;
                background: #ffffff;
                border-radius: 18px;
                padding: 1rem;
                box-shadow: 0 24px 70px rgba(15, 23, 42, 0.38);
                display: flex;
                flex-direction: column;
                align-items: center;
            }
            .slide-modal-close {
                position: absolute;
                top: 0.7rem;
                right: 0.8rem;
                width: 2rem;
                height: 2rem;
                border-radius: 999px;
                display: flex;
                align-items: center;
                justify-content: center;
                background: #0f172a;
                color: #ffffff;
                font-size: 1.3rem;
                font-weight: 700;
                line-height: 1;
                cursor: pointer;
                z-index: 2;
            }
            .slide-modal-image {
                max-width: 100%;
                max-height: 82vh;
                object-fit: contain;
                border-radius: 12px;
                display: block;
            }
            .slide-modal-caption {
                color: #334155;
                font-size: 0.95rem;
                text-align: center;
                margin-top: 0.55rem;
            }
            .slide-image-toggle:checked ~ .slide-modal {
                display: flex;
            }
            @media (max-width: 900px) {
                h1 {font-size: 1.9rem !important;}
                .metric-row {grid-template-columns: 1fr;}
                .slide-image {max-height: 260px;}
                .slide-modal {padding: 2vh 2vw;}
                .slide-modal-content {width: 96vw; padding: 0.75rem;}
                .slide-modal-image {max-height: 78vh;}
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def slide_header(title: str, subtitle: str, lead: str):
    st.title(title)
    st.markdown(f'<div class="slide-subtitle">{subtitle}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="slide-lead">{lead}</div>', unsafe_allow_html=True)


def bullets(items):
    for item in items:
        st.markdown(f'<div class="slide-card">{item}</div>', unsafe_allow_html=True)


def _safe_image_id(filename: str) -> str:
    stem = re.sub(r"[^a-zA-Z0-9_-]+", "-", filename)
    return f"slide-img-{stem}"


def show_image(filename: str, caption: str | None = None):
    """Affiche une image compacte cliquable avec ouverture en popup CSS.

    Le clic sur l'image ouvre une vue agrandie. La fermeture se fait via
    le bouton × ou en cliquant sur le fond sombre.
    """
    path = ASSETS_DIR / filename
    if path.exists():
        mime = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
        data = base64.b64encode(path.read_bytes()).decode("utf-8")
        alt = html.escape(caption or filename)
        image_id = _safe_image_id(filename)
        caption_html = f'<div class="slide-caption">{alt}</div>' if caption else ""
        modal_caption_html = f'<div class="slide-modal-caption">{alt}</div>' if caption else ""

        st.markdown(
            f"""
            <input class="slide-image-toggle" type="checkbox" id="{image_id}" />
            <label class="slide-image-wrap" for="{image_id}" title="Cliquer pour agrandir">
                <img class="slide-image" src="data:{mime};base64,{data}" alt="{alt}" />
            </label>
            {caption_html}
            <div class="slide-modal" aria-label="Image agrandie">
                <label class="slide-modal-backdrop" for="{image_id}" title="Fermer"></label>
                <div class="slide-modal-content">
                    <label class="slide-modal-close" for="{image_id}" title="Fermer">×</label>
                    <img class="slide-modal-image" src="data:{mime};base64,{data}" alt="{alt}" />
                    {modal_caption_html}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.warning(f"Image introuvable : {path}")
