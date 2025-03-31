// fileWriter.js
const fs = require("fs").promises;

// Listen for messages from parent
process.on("message", async (message) => {
  try {
    if (message.type === "write_file") {
      const { filename, data } = message;

      // Create directory if needed
      const dirname = require("path").dirname(filename);
      await fs.mkdir(dirname, { recursive: true });

      // Write the file
      await fs.writeFile(filename, JSON.stringify(data, null, 2));

      // Confirm success
      process.send({
        type: "write_complete",
        filename,
        size: data.length || Object.keys(data).length,
      });
    }
  } catch (error) {
    process.send({
      type: "write_error",
      filename: message.filename,
      error: error.message,
    });
  }
});

// Signal ready
process.send({ type: "ready" });
