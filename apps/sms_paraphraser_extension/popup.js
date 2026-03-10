/**
 * popup.js
 * Handles UI logic and triggers extraction/paraphrasing.
 */

let extractedData = {};

document.addEventListener('DOMContentLoaded', async () => {
    const generateBtn = document.getElementById('generate-btn');
    const copyBtn = document.getElementById('copy-btn');
    const outputText = document.getElementById('output-text');
    const resultContainer = document.getElementById('result-container');
    const loading = document.getElementById('loading');
    const errorMsg = document.getElementById('error-msg');
    const sourceStatus = document.getElementById('data-source-status');

    // Request extraction from content script
    async function requestExtraction(tabId) {
        loading.style.display = 'block';
        loading.textContent = "Connecting to page...";
        errorMsg.style.display = 'none';

        chrome.tabs.sendMessage(tabId, { action: "extractData" }, async (response) => {
            if (chrome.runtime.lastError) {
                console.warn("SMS Paraphraser: Connection failed, attempting injection...", chrome.runtime.lastError.message);

                // As a fallback, try to inject the script manually
                chrome.scripting.executeScript({
                    target: { tabId: tabId },
                    files: ['content.js']
                }).then(() => {
                    // Try sending the message again after a short delay
                    setTimeout(() => {
                        chrome.tabs.sendMessage(tabId, { action: "extractData" }, async (retryResponse) => {
                            if (chrome.runtime.lastError) {
                                loading.style.display = 'none';
                                sourceStatus.style.display = 'none';
                                errorMsg.textContent = "Error: Failed to connect. Refresh the page.";
                                errorMsg.style.display = 'block';
                            } else if (retryResponse && retryResponse.data) {
                                await processExtractedData(retryResponse.data);
                            } else {
                                loading.style.display = 'none';
                                errorMsg.textContent = "Extraction Error: No data received from page after injection.";
                                errorMsg.style.display = 'block';
                            }
                        });
                    }, 100);
                }).catch(err => {
                    loading.style.display = 'none';
                    errorMsg.textContent = "Injection Error: " + err.message;
                    errorMsg.style.display = 'block';
                });
                return;
            }

            console.log("SMS Paraphraser: Received response", response);
            if (response && response.data) {
                await processExtractedData(response.data);
            } else {
                loading.style.display = 'none';
                errorMsg.textContent = "Extraction Error: No data received.";
                errorMsg.style.display = 'block';
            }
        });
    }

    function stripHtml(html) {
        if (!html) return "";
        const doc = new DOMParser().parseFromString(html, 'text/html');
        return doc.body.textContent || "";
    }

    async function processExtractedData(data) {
        console.log("SMS Paraphraser: Data extracted from page:", data);
        extractedData = data;
        const eventId = data.eventId;

        if (eventId) {
            console.log("SMS Paraphraser: Event ID found in URL:", eventId);
            loading.style.display = 'block';
            loading.textContent = "Fetching database details...";
            generateBtn.disabled = true;
            generateBtn.textContent = "Loading DB...";

            try {
                // Fetch from Localhost/Production API
                // We use the full URL to ensure it hits the FastAPI backend
                const apiResponse = await fetch(`http://localhost:8000/reportable/event/${eventId}`);
                if (apiResponse.ok) {
                    const dbData = await apiResponse.json();
                    console.log("SMS Paraphraser: Success! DB Data Received:", dbData);

                    // PURE DB OVERRIDE for requested fields
                    // Use a slightly more defensive update pattern
                    Object.assign(extractedData, {
                        "Event Name": dbData.title || extractedData["Event Name"],
                        "Venue Details": stripHtml(dbData.venue_details),
                        "Parking Note": stripHtml(dbData.parking_note),
                        "Directions": stripHtml(dbData.directions),
                        "Check-In": stripHtml(dbData.check_in),
                        "Venue Name": dbData.venue_name || dbData.title || "[Venue]",
                        "Date": dbData.date || extractedData["Date"],
                        "Location": dbData.location || extractedData["Location"]
                    });

                    sourceStatus.textContent = "Source: Database (" + eventId + ")";
                    sourceStatus.className = "status-badge status-found";
                    sourceStatus.style.display = "block";

                    console.log("SMS Paraphraser: Final extraction data for UI:", extractedData);
                } else {
                    console.warn("SMS Paraphraser: Event not found in DB via API. Status:", apiResponse.status);
                    sourceStatus.textContent = "Source: Page Only (DB ID Not Found)";
                    sourceStatus.className = "status-badge status-missing";
                    sourceStatus.style.display = "block";
                }
            } catch (err) {
                console.error("SMS Paraphraser: API Fetch detailed error:", {
                    message: err.message,
                    name: err.name,
                    stack: err.stack
                });
                sourceStatus.textContent = "Source: Page Only (API Connection Error)";
                sourceStatus.className = "status-badge status-missing";
                sourceStatus.style.display = "block";
            } finally {
                generateBtn.disabled = false;
                generateBtn.textContent = "Generate SMS";
                loading.style.display = 'none';
            }
        } else {
            console.warn("SMS Paraphraser: No Event ID found in URL parameters.");
            sourceStatus.textContent = "Source: Page Only (No Event ID)";
            sourceStatus.className = "status-badge status-missing";
            sourceStatus.style.display = "block";
            loading.style.display = 'none';
        }

        updateUI(extractedData);
    }

    try {
        const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

        if (!tab.url.includes('golive.culinarystaffing.com')) {
            errorMsg.textContent = "Please use this on the GoLive website.";
            errorMsg.style.display = 'block';
            return;
        }

        requestExtraction(tab.id);
    } catch (err) {
        loading.style.display = 'none';
        errorMsg.textContent = "Error: " + err.message;
        errorMsg.style.display = 'block';
    }

    function updateUI(data) {
        let hasAny = false;
        const dbFields = ["Event Name", "Venue Details", "Parking Note", "Directions", "Check-In"];
        const scrapedFields = ["Date", "Time", "Location"];
        const allFields = [...dbFields, ...scrapedFields];

        for (const key of allFields) {
            const value = data[key];
            const checkbox = document.getElementById(key);

            // Always allow selection as requested
            checkbox.disabled = false;

            // Auto-check only if there is actually data
            if (value && value.toString().trim().length > 0) {
                checkbox.checked = true;
                hasAny = true;
            } else {
                checkbox.checked = false;
            }
        }

        // Always allow generation attempt
        generateBtn.disabled = false;
    }

    generateBtn.addEventListener('click', async () => {
        const selectedSections = [];
        document.querySelectorAll('input[type="checkbox"]:checked').forEach(cb => {
            selectedSections.push({
                title: cb.value,
                text: extractedData[cb.value] || ""
            });
        });

        // Always include Venue Name if we have it from the DB
        const vNameLabel = "Venue Name";
        const finalVenueName = extractedData[vNameLabel] || extractedData["Event Name"] || "[Venue]";

        // Ensure Venue Name is in the list for background.js to find
        selectedSections.push({
            title: vNameLabel,
            text: finalVenueName
        });

        console.log("SMS Paraphraser: Final Payload Audit sent to background:", JSON.stringify({
            eventId: extractedData.eventId,
            venueName: finalVenueName,
            sections: selectedSections.map(s => ({ title: s.title, hasText: !!s.text, textPreview: s.text ? s.text.substring(0, 30) + "..." : "EMPTY" }))
        }, null, 2));

        if (selectedSections.length === 0) {
            console.warn("SMS Paraphraser: No sections selected.");
            return;
        }

        generateBtn.disabled = true;
        generateBtn.textContent = "Paraphrasing...";
        errorMsg.style.display = 'none';

        try {
            const response = await chrome.runtime.sendMessage({
                action: "paraphrase",
                eventId: extractedData.eventId,
                sections: selectedSections
            });

            if (response.error) {
                throw new Error(response.error);
            }

            outputText.value = response.text;
            resultContainer.style.display = 'block';
        } catch (err) {
            errorMsg.textContent = "API Error: " + err.message;
            errorMsg.style.display = 'block';
        } finally {
            generateBtn.disabled = false;
            generateBtn.textContent = "Generate SMS";
        }
    });

    copyBtn.addEventListener('click', () => {
        outputText.select();
        navigator.clipboard.writeText(outputText.value);
        copyBtn.textContent = "Copied!";
        setTimeout(() => {
            copyBtn.textContent = "Copy to Clipboard";
        }, 2000);
    });
});
