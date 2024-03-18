# Use an official Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the source code into the container
COPY /src/tasks/a_splitter /app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default command to run when the container starts
CMD ["python", "-m", "luigi", "--module", "a_splitter", "Splitter", "--local-scheduler"]

