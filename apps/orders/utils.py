import re

def clean_email_text(text: str) -> str:
    """
    Cleans raw email or order text before it is processed by AI.
    It removes:
    - [cid:...] image indicators and trailing URLs.
    - [A logo / image description    AI-generated content may be incorrect.] warning tags.
    - Standalone social media and podcast links.
    - Promotional footer blocks (e.g. "COMING SOON", renaming notices, P.S. notes).
    - Confidentiality disclaimers.
    - Excess blank lines.
    """
    if not text:
        return ""

    # Normalize smart quotes and dashes to standard ones
    text = (text.replace("\u2018", "'")
                .replace("\u2019", "'")
                .replace("\u201c", '"')
                .replace("\u201d", '"')
                .replace("\u2013", "-")
                .replace("\u2014", "--"))

    # 1. Remove [cid:...] and optional trailing URLs (standard link wrappers like <http...>)
    text = re.sub(
        r'\[cid:[^\]]+\](?:<https?://[^>]+>|https?://\S+)?',
        '',
        text
    )

    # 2. Remove [logo/image AI-generated content may be incorrect] warning tags
    text = re.sub(
        r'\[[^\]]*(?:logo|AI-generated|image|content may be incorrect)[^\]]*\]',
        '',
        text,
        flags=re.IGNORECASE
    )

    # 3. Remove standalone/inline social and podcast links
    social_domains = (
        r'facebook\.com|instagram\.com|linkedin\.com|twitter\.com|'
        r'youtube\.com|spotify\.com|apple\.com/us/podcast'
    )
    text = re.sub(
        r'<?https?://(?:www\.)?(?:' + social_domains + r')\S*>?',
        '',
        text,
        flags=re.IGNORECASE
    )

    # 4. Remove multi-line "coming soon" branding/banners
    text = re.sub(r'(?i)coming\s+soon\W*', '', text)

    # Split into paragraphs to process blocks
    paragraphs = re.split(r'\n\s*\n', text)
    cleaned_paragraphs = []
    
    # Track if we hit a confidentiality disclaimer at the bottom
    disclaimer_indicators = [
        "this email and any files",
        "confidentiality notice",
        "the information contained in this",
        "legally privileged",
        "please consider the environment"
    ]

    promo_indicators = [
        "coming soon",
        "become golive! staffing",
        "same team and ownership",
        "expanding into more locations",
        "aligning with our golive! app",
        "p.s. while i have you"
    ]

    for paragraph in paragraphs:
        stripped = paragraph.strip()
        if not stripped:
            continue
            
        # Check if we should truncate from this point onward due to disclaimers
        stripped_lower = stripped.lower()
        if any(ind in stripped_lower for ind in disclaimer_indicators):
            break

        # Check if this paragraph contains promotional phrases to skip
        if any(ind in stripped_lower for ind in promo_indicators):
            continue

        # Check if paragraph has "coming" and "soon" split across lines
        # E.g. COMING\n SOON....
        normalized_para = re.sub(r'\s+', ' ', stripped_lower)
        if "coming soon" in normalized_para:
            continue

        # If it's just a single character or empty after cleaning
        if not stripped:
            continue

        cleaned_paragraphs.append(paragraph.strip())

    # Re-join paragraphs
    cleaned_text = "\n\n".join(cleaned_paragraphs).strip()

    # Normalize consecutive newlines
    cleaned_text = re.sub(r'\n{3,}', '\n\n', cleaned_text)

    return cleaned_text
