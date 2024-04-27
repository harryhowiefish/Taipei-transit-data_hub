FROM mysql:8.0

# Set environment variables for MySQL
ENV MYSQL_DATABASE=group2_db
ENV MYSQL_ROOT_PASSWORD=password

# Optional: Set the default port for MySQL
EXPOSE 3306

# Optional: Mount a volume for persistent data storage
# VOLUME ["/var/lib/mysql"]

# The default command runs MySQL
CMD ["mysqld"]