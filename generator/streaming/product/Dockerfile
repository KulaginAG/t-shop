# Указываем базовый образ
FROM python:3.9-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файлы проекта в контейнер
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip install -r requirements.txt

# Копируем оставшиеся файлы проекта
COPY . .

# Определяем команду для запуска приложения
CMD ["python", "app.py"]
