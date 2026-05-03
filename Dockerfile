# Используем официальный образ Python
FROM python:3.11-slim

# Устанавливаем системные зависимости для работы с Postgres
RUN apt-get update && \
    apt-get install -y postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файл зависимостей и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код проекта
COPY . .

# Команда запуска бота
CMD ["python", "main.py"]
