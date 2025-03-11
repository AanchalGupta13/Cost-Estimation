AWS.config.update({
    accessKeyId: "AWS_AccessKeyId",
    secretAccessKey: "AWS_SecreAccessKey",
    region: "us-east-1"
});

const s3 = new AWS.S3();
const BUCKET_NAME = "price-inventory";

// Upload File to S3
function uploadFile() {
    let file = document.getElementById("fileInput").files[0];
    if (!file) {
        alert("Please select a file first!");
        return;
    }

    let params = {
        Bucket: BUCKET_NAME,
        Key: file.name,
        Body: file
    };

    document.getElementById("uploadStatus").innerText = "Uploading...";
    
    s3.upload(params, function(err, data) {
        if (err) {
            alert("Upload failed: " + err.message);
        } else {
            document.getElementById("uploadStatus").innerText = "Upload Successful!";
            listFiles(); // Refresh file list
        }
    });
}

// List Files in S3
function listFiles() {
    let params = {
        Bucket: BUCKET_NAME
    };

    s3.listObjects(params, function(err, data) {
        if (err) {
            alert("Error fetching files: " + err.message);
        } else {
            let fileList = document.getElementById("fileList");
            fileList.innerHTML = "";

            data.Contents.forEach(function(file) {
                let li = document.createElement("li");
                li.innerHTML = `
                    ${file.Key} 
                    <button onclick="downloadFile('${file.Key}')">Download</button>
                `;
                fileList.appendChild(li);
            });
        }
    });
}

// Download File from S3
function downloadFile(fileName) {
    let params = {
        Bucket: BUCKET_NAME,
        Key: fileName
    };

    s3.getSignedUrl("getObject", params, function(err, url) {
        if (err) {
            alert("Error generating download link: " + err.message);
        } else {
            window.location.href = url;
        }
    });
}

// Load files on page load
window.onload = listFiles;
