# Use the official MongoDB image from Docker Hub
FROM mongo:7.0-rc

# Set environment variables for MongoDB
ENV MONGO_INITDB_ROOT_USERNAME=root
ENV MONGO_INITDB_ROOT_PASSWORD=password

# Optional: Set the default port for MongoDB
EXPOSE 27017

# Optional: Mount a volume for persistent data storage
# VOLUME ["/data/db"]

# The default command runs MongoDB with the entrypoint script
CMD ["mongod"]