// app.js

document.getElementById("uploadForm").addEventListener("submit", async function (e) {
    e.preventDefault();

    // Get references to the elements
    const resultBox = document.getElementById("resultBox");
    const loadingAnimation = document.getElementById("loadingAnimation");
    const successMessage = document.getElementById("successMessage");

    // Show loading animation and hide success message
    loadingAnimation.style.display = "block";  // Show the loading spinner
    successMessage.style.display = "none";     // Hide the success message
    resultBox.textContent = "Uploading & processing...";  // Show uploading message

    // Get the files and persist checkbox value
    const files = document.getElementById("files").files;
    const persist = document.getElementById("persist").checked;

    if (!files.length) {
        resultBox.textContent = "Please select a PDF file first.";  // Error if no files selected
        return;
    }

    const formData = new FormData();

    // Determine endpoint based on number of files
    const isMulti = files.length > 1;
    const endpoint = isMulti ? "/ingest/batch" : "/ingest/file";

    // Append files to FormData
    for (let f of files) {
        formData.append(isMulti ? "files" : "file", f);
    }

    // Append the persist value to FormData
    formData.append("persist", persist);

    try {
        // Make the API call to upload the files
        const response = await fetch(endpoint, {
            method: "POST",
            body: formData
        });

        const text = await response.text();  // Get the response text

        // Try to parse the response as JSON and update the resultBox
        try {
            resultBox.textContent = JSON.stringify(JSON.parse(text), null, 2);  // Format JSON response
            loadingAnimation.style.display = "none";  // Hide the loading spinner
            successMessage.style.display = "block";  // Show the success message
        } catch (err) {
            resultBox.textContent = text;  // If response is not JSON, show the raw text
            loadingAnimation.style.display = "none";  // Hide the loading spinner
            successMessage.style.display = "block";  // Show the success message
        }

    } catch (err) {
        // Handle error during fetch
        resultBox.textContent = "Error: " + err;  // Show error message
        loadingAnimation.style.display = "none";  // Hide the loading spinner
    }
});
