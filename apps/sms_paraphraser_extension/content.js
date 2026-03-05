/**
 * content.js
 * Responsible for extracting specific sections from the GoLive event page.
 */

function getElementByXPath(path) {
    try {
        return document.evaluate(path, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
    } catch (e) {
        console.error(`SMS Paraphraser: XPath error for ${path}`, e);
        return null;
    }
}

function extractEventData() {
    console.log("SMS Paraphraser: Starting unified extraction...");

    const results = {};

    // Helper to find value by label text (MUY Typography style)
    function getValueByLabel(labels) {
        const allTypography = Array.from(document.querySelectorAll('.MuiTypography-root, label, h1, h2, h3, h4, h5, h6, b, strong, span'));

        for (const el of allTypography) {
            const text = el.innerText.trim();
            if (labels.some(l => text === l || text.toLowerCase() === l.toLowerCase())) {
                console.log(`SMS Paraphraser: Found label element for`, labels, el);

                // Strategy 1: Related CKEditor (Edit Mode)
                const container = el.closest('.MuiPaper-root') || el.parentElement;
                const editor = container?.querySelector('.ck-editor__editable');
                if (editor && editor.innerText.trim().length > 5) return editor.innerText.trim();

                // Strategy 2: Next sibling(s) with text (View Mode)
                // We look for the first sibling that isn't the label itself and has text
                let next = el.nextElementSibling;
                let contentFound = null;
                while (next) {
                    const nextText = next.innerText.trim();
                    if (nextText.length > 0 && !labels.includes(nextText)) {
                        // For Parking Note, we might need to skip "Street Parking" if there's more specific text below
                        if (labels.includes("PARKING NOTE") && nextText.toLowerCase().includes("street parking")) {
                            // Keep looking for a more specific note
                            contentFound = nextText;
                        } else {
                            return nextText;
                        }
                    }
                    next = next.nextElementSibling;
                }

                // Strategy 3: Parent's next sibling (View Mode)
                let parentNext = el.parentElement?.nextElementSibling;
                if (parentNext && parentNext.innerText.trim().length > 0) return parentNext.innerText.trim();

                return contentFound; // Return the less-specific one if nothing better found
            }
        }
        return null;
    }

    // Define the core fields we still want to grab from the page if possible (as fallbacks)
    const fieldDefinitions = {
        "Event Name": ["Title", "Event Name"],
        "Date": ["Event Date", "Date"],
        "Location": ["Address", "Location"]
    };

    for (const [name, labels] of Object.entries(fieldDefinitions)) {
        // Try Edit Mode (Inputs)
        if (name === "Event Name") results[name] = document.querySelector('input[name="title"]')?.value;
        else if (name === "Date") results[name] = document.querySelector('input[name="event_date"]')?.value;
        else if (name === "Location") results[name] = document.querySelector('input[name="address1"]')?.value;

        // If not found, a very simple label search (shorter than before)
        if (!results[name]) {
            const allTypography = Array.from(document.querySelectorAll('.MuiTypography-root, label, b, span'));
            for (const el of allTypography) {
                if (labels.some(l => el.innerText.trim().toLowerCase() === l.toLowerCase())) {
                    let next = el.nextElementSibling;
                    if (next && next.innerText.trim().length > 0) {
                        results[name] = next.innerText.trim();
                        break;
                    }
                }
            }
        }
    }

    // Still need Time from the Shifts tab
    results["Time"] = getElementByXPath("//div[contains(text(), 'Shifts')]/..//div[contains(text(), '-')]")?.innerText.trim();

    // Extract Event ID from URL (CRITICAL)
    const urlParams = new URLSearchParams(window.location.search);
    let eventId = urlParams.get('id');

    // Fallback: Try to get ID from path if not in query string (e.g., /events/166589/view)
    if (!eventId) {
        const pathSegments = window.location.pathname.split('/');
        for (const segment of pathSegments) {
            // Check if segment is numeric and has a reasonable length (at least 5 digits)
            if (segment && !isNaN(segment) && segment.length >= 5) {
                eventId = segment;
                break;
            }
        }
    }

    results["eventId"] = eventId;

    console.log("SMS Paraphraser: Minimal Results:", results);
    return results;
}


// Listen for messages from the popup
chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "extractData") {
        console.log("SMS Paraphraser: Message received from popup");
        const data = extractEventData();
        sendResponse({ data: data });
    }
    return true;
});
