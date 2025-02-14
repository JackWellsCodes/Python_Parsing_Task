import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import matplotlib.pyplot as plt
import seaborn as sns

# Constants
KEYWORDS = [
    'Machine Learning', 'Artificial Intelligence', 'Deep Learning', 'Neural Networks',
    'Natural Language Processing', 'Computer Vision', 'Reinforcement Learning',
    'Generative Adversarial Networks', 'Supervised Learning', 'Data Science'
]

CSV_FILENAME_AGGREGATED = 'aggregated_books.csv'
CSV_COLUMNS = ['Title', 'First Publish Year', 'Keyword']

TIME_PERIODS = [
    (2002, 2003), (2004, 2005), (2006, 2007),
    (2008, 2009), (2010, 2011), (2012, 2013), (2014, 2015),
    (2016, 2017), (2018, 2019), (2020, 2021), (2022, 2023)
]

MAX_THREADS = 24  # Start with 16 threads

# Constants for file names (Visualization)
OUTPUT_DISTRIBUTION_PLOT = 'distribution_publication_years.png'
OUTPUT_TIME_PERIOD_PLOT = 'books_by_time_period.png'
OUTPUT_KEYWORD_COUNTS = 'book_counts_by_keyword.csv'
OUTPUT_BOOKS_SUMMARY = 'books_summary.csv'


def fetch_books(keyword):
    """
    Fetches book data from the Open Library API based on the given keyword, handling pagination.
    """
    base_url = 'https://openlibrary.org/search.json'
    params = {'title': keyword, 'limit': 1000, 'page': 1}
    all_books = []

    while True:
        try:
            response = requests.get(base_url, params=params, timeout=3)
            response.raise_for_status()
            data = response.json()

            if 'docs' not in data or not data['docs']:
                break

            all_books.extend(data['docs'])

            if len(data['docs']) < params['limit']:
                break

            params['page'] += 1

        except requests.RequestException as e:
            print(f"Request error for '{keyword}': {e}")
            break

    return all_books


def parse_book_titles_and_dates(data, keyword):
    """
    Parses the book data to extract only the titles and first publish year.
    """
    parsed_books = []
    for book in data:
        title = book.get('title', 'N/A')
        first_publish_year = book.get('first_publish_year', 'N/A')
        if isinstance(first_publish_year, int):
            parsed_books.append([title, first_publish_year, keyword])
    return parsed_books


def remove_duplicates(books):
    """
    Removes duplicate books based on title and first publish year.
    """
    seen = set()
    unique_books = []
    for book in books:
        title, year, keyword = book
        identifier = (title, year)
        if identifier not in seen:
            seen.add(identifier)
            unique_books.append(book)
    return unique_books


def count_books_by_period(parsed_books):
    """
    Counts the number of books published in each time period.
    """
    period_counts = {period: 0 for period in TIME_PERIODS}
    for book in parsed_books:
        _, year, _ = book
        for start, end in TIME_PERIODS:
            if start <= year <= end:
                period_counts[(start, end)] += 1
                break
    return period_counts


def save_to_csv(data, filename, columns):
    """
    Saves the parsed book data to a CSV file.
    """
    try:
        df = pd.DataFrame(data, columns=columns)
        df.to_csv(filename, index=False)
        print(f"Data successfully saved to '{filename}'.")
    except IOError as io_err:
        print(f"File I/O error: {io_err}")
    except Exception as e:
        print(f"Unexpected error occurred while saving to CSV: {e}")


def process_keyword(keyword):
    """
    Processes each keyword: fetches data, parses, removes duplicates, and saves to individual CSV.
    """
    print(f"Processing '{keyword}'...")
    books_data = fetch_books(keyword)
    if books_data:
        parsed_books = parse_book_titles_and_dates(books_data, keyword)
        return remove_duplicates(parsed_books)
    return []


def explore_data(df):
    """
    Performs exploration and visualization of the dataset.

    Args:
    - df (DataFrame): The dataset to explore.
    """
    if df.empty:
        print("No data available to explore.")
        return

    # Basic statistics
    print("Basic Statistics:")
    print(f"Total number of books: {len(df)}")
    print(f"Range of publication years: {df['First Publish Year'].min()} - {df['First Publish Year'].max()}")

    # Distribution of publication years
    plt.figure(figsize=(12, 6))
    sns.histplot(df['First Publish Year'], bins=range(df['First Publish Year'].min(), df['First Publish Year'].max() + 1), kde=False)
    plt.title('Distribution of Books by Publication Year')
    plt.xlabel('Publication Year')
    plt.ylabel('Number of Books')
    plt.grid(True)
    plt.savefig(OUTPUT_DISTRIBUTION_PLOT)  # Save the figure
    plt.close()  # Close the plot to avoid display duplication

    # Display the saved figure
    print(f"Displaying {OUTPUT_DISTRIBUTION_PLOT}...")
    plt.figure(figsize=(12, 6))
    img = plt.imread(OUTPUT_DISTRIBUTION_PLOT)
    plt.imshow(img)
    plt.axis('off')  # Hide axes
    plt.show()

    # Updated: Using decade-long time periods
    time_periods = pd.IntervalIndex.from_tuples([
        (2013, 2023), (2003, 2012), (1993, 2002),
        (1983, 1992), (1973, 1982), (1963, 1972),
        (1953, 1962), (1943, 1952), (1933, 1942),
        (1923, 1932), (1913, 1922)
    ])

    df['Time Period'] = pd.cut(df['First Publish Year'], bins=time_periods, include_lowest=True)

    period_counts = df['Time Period'].value_counts().sort_index()

    plt.figure(figsize=(12, 6))
    sns.barplot(x=period_counts.index.astype(str), y=period_counts.values, palette='viridis')
    plt.title('Number of Books by Time Period (Decades)')
    plt.xlabel('Time Period')
    plt.ylabel('Number of Books')
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.savefig(OUTPUT_TIME_PERIOD_PLOT)  # Save the figure
    plt.close()  # Close the plot to avoid display duplication

    # Display the saved figure
    print(f"Displaying {OUTPUT_TIME_PERIOD_PLOT}...")
    plt.figure(figsize=(12, 6))
    img = plt.imread(OUTPUT_TIME_PERIOD_PLOT)
    plt.imshow(img)
    plt.axis('off')  # Hide axes
    plt.show()

    # Summary table of book counts by keyword
    keyword_counts = df['Keyword'].value_counts()
    print("\nBook Counts by Keyword:")
    print(keyword_counts)

    # Save the summary table to a CSV file
    keyword_counts.to_csv(OUTPUT_KEYWORD_COUNTS, header=True)

    # Save the summary table of books to CSV
    df_summary = df[['Title', 'First Publish Year', 'Keyword']].drop_duplicates()
    df_summary.to_csv(OUTPUT_BOOKS_SUMMARY, index=False)
    print(f"Books summary saved to '{OUTPUT_BOOKS_SUMMARY}'.")


def main():
    """
    Main function to execute the script's primary logic.
    """
    start_time = time.time()

    # Part 1: Data fetching and processing
    aggregated_data = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = [executor.submit(process_keyword, keyword) for keyword in KEYWORDS]
        for future in as_completed(futures):
            result = future.result()
            if result:
                aggregated_data.extend(result)

    aggregated_data = remove_duplicates(aggregated_data)
    save_to_csv(aggregated_data, CSV_FILENAME_AGGREGATED, CSV_COLUMNS)

    period_counts = count_books_by_period(aggregated_data)
    print(f"Aggregated book counts by time period:")
    for period, count in period_counts.items():
        print(f"{period}: {count} books")

    # Part 2: Data exploration and visualization
    df = pd.read_csv(CSV_FILENAME_AGGREGATED)  # Load the data
    explore_data(df)  # Explore and visualize the data

    end_time = time.time()
    print(f"Script completed in {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()