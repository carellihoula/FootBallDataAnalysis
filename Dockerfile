# Utiliser une image de base officielle de Python
FROM python:3.9-slim-buster

# Installer les dépendances nécessaires
RUN apt-get update && apt-get install -y build-essential openjdk-11-jdk

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier le fichier requirements.txt et installer les dépendances
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copier le contenu de votre application dans le répertoire de travail
COPY . .

# Exposer le port 8080 pour Streamlit
EXPOSE 8080

# Commande pour exécuter Streamlit sur le port 8080
CMD ["streamlit", "run", "app.py", "--server.port", "8080", "--server.address", "0.0.0.0"]
