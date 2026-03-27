/**
 * background.js
 * Proxy script to handle paraphrasing requests via the local backend.
 * This ensures the OpenAI API key is never exposed in the extension code.
 */

// In production, this extension communicates with our Render backend
// which handles the OpenAI API securely using environment variables.

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "paraphrase") {
        paraphraseViaBackend(request.eventId, request.sections, request.shift_time)
            .then(data => sendResponse({ text: data.text }))
            .catch(error => sendResponse({ error: error.message }));
        return true; // Keep the message channel open for async response
    }
});

/**
 * Sends a request to the FastAPI backend to perform paraphrasing.
 * @param {string|number} eventId - The GoLive Event ID.
 * @param {Array} sections - The selected sections (title and text).
 */
async function paraphraseViaBackend(eventId, sections, shiftTime) {
    // We only need the titles for the backend to know which DB fields to use,
    // but the backend expects a list of strings (titles).
    const sectionTitles = sections.map(s => s.title);

    let payload = {
        event_id: parseInt(eventId),
        sections: sectionTitles
    };

    if (shiftTime) {
        payload.shift_time = shiftTime;
    }

    try {
        const response = await fetch('https://tools.culinarystaffing.com/sms-paraphraser/paraphrase', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            const errorBody = await response.json();
            throw new Error(errorBody.detail || `Backend error: ${response.status}`);
        }

        return await response.json();
    } catch (error) {
        console.error("SMS Paraphraser Background Error:", error);
        throw error;
    }
}
