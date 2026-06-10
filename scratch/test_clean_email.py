import sys
import os

# Set stdout encoding to UTF-8 to prevent Windows terminal encoding errors
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Add the parent directory to Python path to import apps module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from apps.orders.utils import clean_email_text

example_input = """Re: GoLive! Client Feedback - Do Not Return Notice
From: Crystal Hamilton (chamilton@bhusd.org)

5/22

On Thu, May 21, 2026 at 10:56 AM Michael Guess <michael@culinarystaffing.com<mailto:michael@culinarystaffing.com>> wrote:
Thank you for confirming- is the Friday request for tomorrow 5/22? Or for next Friday 5/29. We'll get the order on the board and reach out to Maria to check availability asap.

Thank you,


COMING

 SOON….

[A green and yellow logo    AI-generated content may be incorrect.]



MICHAEL GUESS

CLIENT SUCCESS MANAGER

Office: (323) 965-7582 | Direct: (949) 523-1103

Hours of Operation: 9:00 AM – 5:30 PM, Monday-Friday

[A gold and black logo    AI-generated content may be incorrect.]



[cid:bc231175-b17f-49e0-a2b9-b4e7057e2d4f]<https://www.facebook.com/culinarystaffing/>[cid:57270dfe-0fcf-4f29-8da8-840d04be5cb5]<https://www.instagram.com/culinarystaffing/?hl=en>[cid:11b75101-bc13-4b04-8bb4-8327bcf01335]<https://www.linkedin.com/company/culinary-staffing-service/>[cid:5b1e41da-7086-4f61-8755-46e6dc30bad3]<https://podcasts.apple.com/us/podcast/the-culinary-hustle-overcoming-challenges-embracing/id1763284487>[cid:7c659689-02c0-4e35-a19f-922b5872f623]<https://open.spotify.com/show/7qTpr8x15fkWEN1UCOvClB>

P.S. While I have you…

You’ll start to see a new name and look as we become GoLive! Staffing. It’s still the same team and ownership, just expanding into more locations and industries and aligning with our GoLive! App."""

if __name__ == '__main__':
    print("--- RAW EMAIL INPUT ---")
    print(example_input)
    print("\n--- RUNNING CLEANER ---")
    cleaned = clean_email_text(example_input)
    print("--- CLEANED EMAIL OUTPUT ---")
    print(cleaned)
    print("\n--- ASSERTIONS ---")
    
    # Assertions
    assert "chamilton@bhusd.org" in cleaned, "Sender info should be preserved"
    assert "Michael Guess" in cleaned, "Sender signature name should be preserved"
    assert "MICHAEL GUESS" in cleaned
    assert "CLIENT SUCCESS MANAGER" in cleaned
    assert "Office: (323) 965-7582" in cleaned
    assert "Friday request for tomorrow" in cleaned, "Order details should be preserved"
    
    # Verify junk is removed
    assert "COMING" not in cleaned, "Coming soon branding should be removed"
    assert "SOON" not in cleaned, "Coming soon branding should be removed"
    assert "facebook" not in cleaned, "Social links should be removed"
    assert "instagram" not in cleaned, "Social links should be removed"
    assert "linkedin" not in cleaned, "Social links should be removed"
    assert "cid:" not in cleaned, "CID elements should be removed"
    assert "AI-generated content" not in cleaned, "AI warning content should be removed"
    assert "While I have you" not in cleaned, "P.S. promo note should be removed"
    assert "become GoLive!" not in cleaned, "Renaming promo block should be removed"

    # Test is_simple_acknowledgment from email_monitor
    from apps.orders.email_monitor import is_simple_acknowledgment
    print("Testing is_simple_acknowledgment helper...")
    assert is_simple_acknowledgment("Okay") is True
    assert is_simple_acknowledgment("yep") is True
    assert is_simple_acknowledgment("Thanks!") is True
    assert is_simple_acknowledgment("Looks good.") is True
    assert is_simple_acknowledgment("Ok thanks\nJohn\nSent from my iPhone") is True
    assert is_simple_acknowledgment("Please book 3 servers for tomorrow at 7am") is False

    # Test detect_client_from_text dictation mode
    from apps.orders.knowledge_base import detect_client_from_text
    print("Testing detect_client_from_text dictation mode...")
    # Warner Brothers has ID 1709. chamilton@bhusd.org belongs to BHUSD.
    # Normal mode detects the email and returns BHUSD (not 1709)
    normal_match = detect_client_from_text("This is Warner Brothers. Please send to chamilton@bhusd.org", is_dictation=False)
    # Dictation mode skips email detection and matches Warner Brothers by name (1709)
    dictation_match = detect_client_from_text("This is Warner Brothers. Please send to chamilton@bhusd.org", is_dictation=True)
    
    print(f"Normal match: {normal_match}, Dictation match: {dictation_match}")
    assert normal_match != 1709, "Normal mode should match by email (BHUSD)"
    assert dictation_match == 1709, "Dictation mode should match Warner Brothers by name"
    
    # Test Cedar Sinai match for Cedars Sinai Medical Center (ID 161)
    cedars_match = detect_client_from_text("this order is for Cedar Sinai I need to order two Cooks tonight from 3:30 to midnight", is_dictation=True)
    print(f"Cedar Sinai match: {cedars_match}")
    assert cedars_match == 161, "Should fuzzy match 'Cedar Sinai' to Cedars Sinai Medical Center (ID 161)"
    
    print("SUCCESS: All tests passed!")
