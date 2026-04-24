import os
from pathlib import Path


def count_lines_in_files():
    file_data = []
    
    # Список папок, которые мы точно скипаем
    IGNORE_DIRS = {'node_modules', '__pycache__', 'venv', '.venv', 'env', 'dist', 'build', '.git', '.idea', '.vscode'}

    for path in Path('.').rglob('*.py'):  # Сразу ищем только .py файлы
        # Проверяем, нет ли в пути системных папок или папок на точку
        parts = path.parts
        if any(part.startswith('.') or part in IGNORE_DIRS for part in parts[:-1]):
            continue

        if path.is_file():
            try:
                # Читаем количество строк
                with path.open('r', encoding='utf-8', errors='ignore') as f:
                    line_count = sum(1 for _ in f)
                
                if line_count > 400:
                    file_data.append((str(path), line_count))
                    
            except Exception as e:
                print(f"Ошибка при чтении {path}: {e}")

    # Сортировка по убыванию
    file_data.sort(key=lambda x: x[1], reverse=True)

    # Вывод
    print(f"{'Строк':<10} | {'Путь к файлу'}")
    print("-" * 60)
    for file_path, count in file_data:
        print(f"{count:<10} | {file_path}")

if __name__ == "__main__":
    count_lines_in_files()