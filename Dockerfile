FROM apache/spark:4.0.1

USER root
# Install Python dependencies required for MongoDB integration
RUN pip install --no-cache-dir pymongo

USER 185
