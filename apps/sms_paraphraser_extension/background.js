/**
 * background.js
 * Handles OpenAI API calls to paraphrase text.
 */

// We will hardcode the API key for development as per user instruction
// In a production environment, this should be handled more securely.
// Use the RESUME_ANALYZER_OPENAI_API_KEY as requested by the user
const OPENAI_API_KEY = "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".trim();

chrome.runtime.onMessage.addListener((request, sender, sendResponse) => {
    if (request.action === "paraphrase") {
        paraphraseWithOpenAI(request.sections)
            .then(text => sendResponse({ text: text }))
            .catch(err => sendResponse({ error: err.message }));
        return true; // Keep channel open for async
    }
});

async function paraphraseWithOpenAI(sections) {
    console.log("SMS Paraphraser: Sending request to OpenAI with sections:", sections.map(s => s.title));

    const venueName = sections.find(s => s.title === "Venue Name")?.text || "[Venue]";
    const infoSections = sections.filter(s => s.title !== "Venue Name");

    const prompt = `You are a helpful assistant that paraphrases event details into a concise SMS message.

EXTREMELY IMPORTANT: You must format the SMS EXACTLY like this template, replacing the bracketed items with paraphrased information from the provided data:

"This is your first shift at ${venueName}. Please note the event details:
- Venue: [Paraphrase from 'Venue Details']
- Parking: [Paraphrase from 'Parking Note']
- Directions: [Paraphrase from 'Directions']
- Check-In: [Paraphrase from 'Check-In']"

If a section is missing from the information below, omit that bullet point entirely from the SMS.

Information to paraphrase:
${infoSections.map(s => `${s.title}: ${s.text}`).join('\n')}

Output ONLY the final SMS message. No intro, no outro, no markdown formatting (like backticks or bolding).`;

    console.log("SMS Paraphraser: Final Prompt sent to OpenAI:\n", prompt);

    const url = "https://api.openai.com/v1/chat/completions";
    const headers = {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${OPENAI_API_KEY.trim()}`
    };

    const body = JSON.stringify({
        model: "gpt-4o-mini", // Keeping mini but ensuring no whitespace in key
        messages: [
            { role: "system", content: "You are an expert at writing concise SMS notifications for staffing events." },
            { role: "user", content: prompt }
        ],
        temperature: 0.7
    });

    try {
        const response = await fetch(url, {
            method: "POST",
            headers: headers,
            body: body
        });

        if (!response.ok) {
            const errorData = await response.json();
            console.error("SMS Paraphraser: OpenAI API Error Response:", errorData);
            throw new Error(errorData.error ? errorData.error.message : "API Call failed");
        }

        const data = await response.json();
        return data.choices[0].message.content.trim();
    } catch (error) {
        console.error("SMS Paraphraser: OpenAI Error:", error);
        throw error;
    }
}
