#Part_1
#Best_one as of 2:06pm 12/08/24
import requests
import pandas as pd
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("lotr_books.log"),
        logging.StreamHandler()
    ]
)

# Constants
TITLE = 'lord of the rings'  # Search query for book title
CSV_FILENAME_DETAILS = 'lord_of_the_rings_books_details.csv'  # Output CSV filename for detailed book info
CSV_FILENAME_TITLES = 'lord_of_the_rings_titles.csv'  # Output CSV filename for book titles
CSV_COLUMNS = ['Title', 'Author(s)', 'First Publish Year', 'First Publisher', 'Number of Pages Median']  # Columns for the CSV

def fetch_books(title):
    """
    Fetches books data from the Open Library API based on the given title.
    Handles pagination to retrieve all available results.
    """
    base_url = 'https://openlibrary.org/search.json'  # Base URL for the Open Library API
    books_data = []  # List to store all fetched book data
    page = 1  # Initialize page number for pagination

    while True:
        # Parameters for the API request
        params = {'title': title, 'page': page}
        try:
            # Make the API request
            response = requests.get(base_url, params=params)
            response.raise_for_status()  # Raise an error for bad responses

            try:
                # Parse the response JSON
                data = response.json()

                if page == 1:
                    # Log the total number of books found by the API on the first page
                    logging.info(f"Total number of books found: {data.get('num_found')}")

                # Add the books from this page to the list
                books_data.extend(data['docs'])

                # Break the loop if there are no more results to fetch
                if len(data['docs']) == 0:
                    break
                page += 1  # Move to the next page for additional results

            except ValueError as json_err:
                # Log JSON decoding errors
                logging.error(f"JSON decoding error: {json_err}")
                break
        except requests.ConnectionError:
            # Log network connection errors
            logging.error("Error: Network problem (e.g., DNS failure, refused connection).")
            break
        except requests.Timeout:
            # Log request timeout errors
            logging.error("Error: Request timed out.")
            break
        except requests.HTTPError as http_err:
            # Log HTTP errors (e.g., 404 or 500 status codes)
            logging.error(f"HTTP error occurred: {http_err}")
            break
        except requests.RequestException as e:
            # Log all other request-related errors
            logging.error(f"Request error: {e}")
            break

    return books_data  # Return the collected book data

def parse_books(data):
    """
    Parses detailed book information from the fetched data.
    """
    if not data:
        # Log cases where data is empty or missing 'docs' key
        logging.error("Error: No data or 'docs' key missing in response.")
        return []

    parsed_books = []  # List to store parsed book details
    for book in data:
        # Extract relevant details from each book entry
        title = book.get('title', 'N/A')
        author_name = ", ".join(book.get('author_name', ['N/A']))
        first_publish_year = book.get('first_publish_year', 'N/A')
        publishers = book.get('publisher', ['N/A'])
        first_publisher = publishers[0] if publishers else 'N/A'
        number_of_pages_median = book.get('number_of_pages_median', 'N/A')
        parsed_books.append([title, author_name, first_publish_year, first_publisher, number_of_pages_median])

    return parsed_books  # Return the list of parsed book details

def save_to_csv(data, filename, columns):
    """
    Saves the provided data to a CSV file.
    """
    try:
        # Create a DataFrame from the data
        df = pd.DataFrame(data, columns=columns)
        # Save the DataFrame to a CSV file
        df.to_csv(filename, index=False)
        logging.info(f"Data successfully saved to '{filename}'.")
    except IOError as io_err:
        # Log I/O errors (e.g., file not accessible)
        logging.error(f"File I/O error: {io_err}")
    except Exception as e:
        # Log any other unexpected errors
        logging.error(f"Unexpected error occurred while saving to CSV: {e}")

def main():
    """
    Main function to fetch, parse, and save book data.
    """
    logging.info("Fetching data for 'Lord of the Rings'...")
    data = fetch_books(TITLE)  # Fetch the book data

    if data:
        logging.info(f"Data successfully fetched from API. Total books retrieved: {len(data)}")
    else:
        logging.error("Failed to fetch data.")
        return

    logging.info("Parsing data...")
    books = parse_books(data)  # Extract detailed book information

    if books:
        logging.info("Data successfully parsed.")
    else:
        logging.error("Failed to parse data.")
        return

    # Create a DataFrame for the parsed book data
    df = pd.DataFrame(books, columns=CSV_COLUMNS)

    # Save titles separately
    df_titles = df[['Title']]
    save_to_csv(df_titles, CSV_FILENAME_TITLES, ['Title'])

    # Save the detailed book info
    save_to_csv(df, CSV_FILENAME_DETAILS, CSV_COLUMNS)

    # Print and log the first few rows of titles only
    logging.info("Displaying the first few titles:")
    logging.info(df_titles.head())
    print("First few titles:\n", df_titles.head())

    # Print and log the first few rows of the detailed data (titles + other columns)
    logging.info("Displaying the first few rows of detailed book information:")
    logging.info(df.head())
    print("First few rows of detailed book information:\n", df.head())

if __name__ == "__main__":
    main()  # Execute the main function