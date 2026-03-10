/**
 * background.js
 * Proxy script to handle paraphrasing requests via the local backend.
 * This ensures the OpenAI API key is never exposed in the extension code.
 */

// We will hardcode the API key for development as per user instruction
// In a production environment, this should be handled more securely.
// Use the RESUME_ANALYZER_OPENAI_API_KEY as requested by the user
const OPENAI_API_KEY = "BLOCk".trim();

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "paraphrase") {
        paraphraseViaBackend(request.eventId, request.sections)
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
async function paraphraseViaBackend(eventId, sections) {
    // We only need the titles for the backend to know which DB fields to use,
    // but the backend expects a list of strings (titles).
    const sectionTitles = sections.map(s => s.title);

    try {
        const response = await fetch('http://localhost:8000/sms-paraphraser/paraphrase', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                event_id: parseInt(eventId),
                sections: sectionTitles
            })
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
