import pandas as pd
import requests
import time
import urllib3

# Отключаем предупреждения SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Общие заголовки
HEADERS = {
    'Accept': 'application/json, text/*',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7',
    'Connection': 'keep-alive',
    'Cookie': 'ks.tg=35; k_stat=d802b93e-0388-4fb8-b31c-c1c81eaf2645; kaspi.storefront.cookie.city=750000000',
    'Referer': 'https://kaspi.kz/shop/c/categories/?c=750000000',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'X-KS-City': '750000000',
    'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"'
}


def get_main_categories():
    """Получает список основных категорий"""
    url = 'https://kaspi.kz/yml/main-navigation/n/n/desktop-menu'
    params = {
        'depth': '1',
        'city': '750000000',
        'rootType': 'desktop'
    }

    response = requests.get(url, params=params, headers=HEADERS, verify=False, timeout=10)
    response.raise_for_status()

    data = response.json()
    categories = []

    for node in data['subNodes']:
        categories.append({
            'code': node['code'],
            'title': node['title']
        })

    return categories


def get_category_tree(category_code):
    """Получает дерево подкатегорий для указанной категории"""
    url = 'https://kaspi.kz/yml/product-view/pl/filters'
    params = {
        'q': f':category:{category_code}:availableInZones:Magnum_ZONE1',
        'text': '',
        'all': 'false',
        'sort': 'relevance',
        'ui': 'd',
        'i': '-1',
        'c': '750000000'
    }

    response = requests.get(url, params=params, headers=HEADERS, verify=False, timeout=10)
    response.raise_for_status()

    data = response.json()
    return data.get('data', {}).get('treeCategory', {})


def flatten_categories(items, main_category_code, main_category_title, parent_id=None, parent_title=None, level=0):
    """Рекурсивно разворачивает вложенные категории"""
    rows = []

    for item in items:
        row = {
            'main_category_code': main_category_code,
            'main_category_title': main_category_title,
            'id': item.get('id', ''),
            'title': item.get('title', ''),
            'titleRu': item.get('titleRu', ''),
            'link': item.get('link', ''),
            'active': item.get('active', False),
            'count': item.get('count', 0),
            'popularity': item.get('popularity', 0),
            'expanded': item.get('expanded', False),
            'parent_id': parent_id,
            'parent_title': parent_title,
            'level': level
        }
        rows.append(row)

        # Рекурсивно обрабатываем подкатегории
        if 'items' in item and item['items']:
            child_rows = flatten_categories(
                item['items'],
                main_category_code=main_category_code,
                main_category_title=main_category_title,
                parent_id=item.get('id'),
                parent_title=item.get('title'),
                level=level + 1
            )
            rows.extend(child_rows)

    return rows


def collect_all_categories(delay=1.0):
    """Собирает все категории со всех основных разделов"""
    print("Получаем список основных категорий...")
    main_categories = get_main_categories()
    print(f"Найдено {len(main_categories)} основных категорий\n")

    all_rows = []

    for i, cat in enumerate(main_categories, 1):
        code = cat['code']
        title = cat['title']
        print(f"[{i}/{len(main_categories)}] Обрабатываем: {title} ({code})...", end=' ')

        try:
            tree = get_category_tree(code)

            # Добавляем top категорию если есть
            if 'top' in tree and tree['top']:
                top = tree['top']
                all_rows.append({
                    'main_category_code': code,
                    'main_category_title': title,
                    'id': top.get('id', ''),
                    'title': top.get('title', ''),
                    'titleRu': top.get('titleRu', ''),
                    'link': top.get('link', ''),
                    'active': top.get('active', False),
                    'count': top.get('count', 0),
                    'popularity': top.get('popularity', 0),
                    'expanded': top.get('expanded', False),
                    'parent_id': None,
                    'parent_title': None,
                    'level': -1  # top уровень
                })

            # Обрабатываем items
            if 'items' in tree and tree['items']:
                rows = flatten_categories(tree['items'], code, title)
                all_rows.extend(rows)
                print(f"OK ({len(rows)} подкатегорий)")
            else:
                print("OK (нет подкатегорий)")

        except Exception as e:
            print(f"ОШИБКА: {e}")

        # Задержка между запросами
        if i < len(main_categories):
            time.sleep(delay)

    return all_rows


def main():
    print("=" * 60)
    print("Сбор всех категорий Kaspi.kz")
    print("=" * 60 + "\n")

    # Собираем данные
    all_data = collect_all_categories(delay=0.5)

    # Создаём DataFrame
    df = pd.DataFrame(all_data)

    print("\n" + "=" * 60)
    print(f"Итого собрано: {len(df)} категорий")
    print("=" * 60)

    # Статистика по основным категориям
    print("\nСтатистика по основным категориям:")
    stats = df.groupby('main_category_title').size().sort_values(ascending=False)
    print(stats.to_string())

    # Сохраняем в CSV
    output_file = 'kaspi_all_categories.csv'
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\nДанные сохранены в: {output_file}")

    # Также сохраняем в Excel если нужно
    try:
        excel_file = 'kaspi_all_categories.xlsx'
        df.to_excel(excel_file, index=False)
        print(f"Данные сохранены в: {excel_file}")
    except Exception as e:
        print(f"Не удалось сохранить Excel: {e}")

    return df


if __name__ == '__main__':
    df = main()
