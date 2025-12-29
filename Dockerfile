# Use the official Apify Python image
FROM apify/actor-python:3.11

# Copy all files to the container
COPY . ./

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Run the main.py script
CMD ["python", "main.py"]