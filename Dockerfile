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

# Exposer le port sur lequel Streamlit fonctionne
EXPOSE 8501

# Commande pour exécuter Streamlit
CMD ["streamlit", "run", "app.py"]
