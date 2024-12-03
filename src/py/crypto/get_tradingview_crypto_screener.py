import requests
import time
import re
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC


def find_table_container(driver):
    container_selectors = [
        '.tableContainer-[a-zA-Z0-9]+',  # Regex for class starting with 'tableContainer-'
        'div[class*="tableContainer"]',  # Any div with class containing 'tableContainer'
        'div[data-role="grid"]',
        '.tv-screener__content-pane'
    ]
    
    for selector in container_selectors:
        elements = driver.find_elements(By.CSS_SELECTOR, selector)
        if elements:
            return elements[0]
    return None

def find_table_element(driver):
    """Find the table using multiple strategies"""
    
    # Execute JS to ensure the page is fully loaded and table is visible
    js_script = """
    // Scroll to ensure everything is loaded
    window.scrollTo(0, document.body.scrollHeight);
    
    // Find table using common patterns
    let table = document.querySelector('table');
    
    // If standard table not found, look for div-based tables
    if (!table) {
        table = document.querySelector('div[role="grid"]') ||
                document.querySelector('div[data-role="grid"]') ||
                document.querySelector('div.tv-screener__content-pane');
    }
    
    return table;
    """
    
    # First try: Execute JavaScript to find table
    table = driver.execute_script(js_script)
    if table:
        return table
        
    # Second try: Look for table by structure
    try:
        # Look for elements that commonly indicate a table
        selectors = [
            'table',
            'div[role="grid"]',
            'div[data-role="grid"]',
            'div.tv-screener__content-pane',
            'div[class*="table"]'  # Look for any class containing "table"
        ]
        
        for selector in selectors:
            elements = driver.find_elements(By.CSS_SELECTOR, selector)
            for element in elements:
                # Verify if it looks like our target table
                if verify_table_structure(element):
                    return element
                    
    except Exception as e:
        print(f"Error finding table by structure: {str(e)}")
    
    return None

def verify_table_structure(element):
    """Verify if the element has the expected structure of our target table"""
    try:
        # Check if element contains typical crypto table data
        text = element.get_attribute('innerHTML').lower()
        
        # Look for common crypto-related terms
        crypto_indicators = ['btc', 'eth', 'market cap', 'volume', 'price']
        has_crypto_terms = any(term in text for term in crypto_indicators)
        
        # Look for table-like structure
        has_rows = bool(element.find_elements(By.CSS_SELECTOR, 'tr, [role="row"]'))
        has_cells = bool(element.find_elements(By.CSS_SELECTOR, 'td, th, [role="gridcell"], [role="columnheader"]'))
        
        return has_crypto_terms and has_rows and has_cells
        
    except Exception:
        return False

def dismiss_popups(driver):
    """Intelligently find and dismiss modal popups"""
    try:
        # Look for the modal container
        modal_script = """
        return document.querySelector('.tv-dialog__modal-wrap') || 
               document.querySelector('.tv-dialog__modal-container') ||
               document.querySelector('.js-dialog');
        """
        modal = driver.execute_script(modal_script)
        
        if modal:
            print("Found modal popup")
            # Look for the close button within the modal
            close_script = """
            let modal = arguments[0];
            let closeButton = modal.querySelector('.tv-dialog__close') ||
                            modal.querySelector('.js-dialog__close') ||
                            modal.querySelector('[class*="dialog-close"]');
                            
            if (closeButton) {
                closeButton.click();
                return true;
            }
            return false;
            """
            
            closed = driver.execute_script(close_script, modal)
            if closed:
                print("Successfully closed modal popup")
                time.sleep(1)  # Wait for animation
                return True
            
            # If no close button found, try removing the modal directly
            remove_script = """
            let modal = arguments[0];
            modal.remove();
            document.body.classList.remove('i-with-dialog');
            return true;
            """
            driver.execute_script(remove_script, modal)
            print("Removed modal popup via DOM manipulation")
            return True
            
    except Exception as e:
        print(f"Error handling popup: {str(e)}")
        return False

def find_table_rows(driver):
    row_selectors = [
        '[class^="row-"][class$="RdUXZpkv"]',  # Classes starting with 'row-' and ending with 'RdUXZpkv'
        'div[data-role="row"]',
        'tr'
    ]
    
    for selector in row_selectors:
        rows = driver.find_elements(By.CSS_SELECTOR, selector)
        if rows:
            return rows
    return []

def scroll_table(driver, table_element):
    """Scroll through the table body to load all data, simulating mouse movement and scrolling"""
    try:
        # First check and dismiss any initial popups
        popup_check_script = """
        return Boolean(
            document.querySelector('.tv-dialog__modal-wrap') || 
            document.querySelector('.tv-dialog__modal-container') ||
            document.querySelector('.js-dialog')
        );
        """
        
        has_popup = driver.execute_script(popup_check_script)
        if has_popup:
            dismiss_popups(driver)
            time.sleep(1)  # Wait for popup to clear
            
        # Find the scrollable container
        scrollable_container = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, 'tableContainer-OuXcFHzP'))
        )
        
        if not scrollable_container:
            print("Could not find scrollable container")
            return 0

        # Create ActionChains object
        actions = ActionChains(driver)

        # Move to the scrollable container
        actions.move_to_element(scrollable_container).perform()

        scroll_count = 0
        max_scrolls = 50
        previous_row_count = 0

        while scroll_count < max_scrolls:
            has_popup = driver.execute_script(popup_check_script)
            if has_popup:
                dismiss_popups(driver)
                time.sleep(1)  
                
            current_rows = driver.find_elements(By.CLASS_NAME, 'row-RdUXZpkv')
            current_row_count = len(current_rows)
            print(f"Current row count: {current_row_count}")

            # Scroll using ActionChains
            actions.move_to_element(scrollable_container)
            
            
            # Alternative scroll method using JavaScript
            # actions.click_and_hold().move_by_offset(0, 100).release().perform()
            #driver.execute_script("arguments[0].scrollTop += arguments[0].clientHeight;", scrollable_container)
            # actions.scroll_by_amount(0, 300).perform()  # Scroll down by 300 pixels

            # Click on the last visible row to ensure focus
            if current_rows:
                last_visible_row = current_rows[-1]
                actions.move_to_element(last_visible_row).click().perform()
                time.sleep(0.5)  # Short pause after click

            # Simulate pressing the DOWN key multiple times
            for _ in range(50):
                actions.send_keys(Keys.ARROW_DOWN).perform()
                time.sleep(0.05)  # Short pause between key presses

            # Wait for new data to load
            time.sleep(2)

            # Check if we've reached the bottom
            new_row_count = len(driver.find_elements(By.CLASS_NAME, 'row-RdUXZpkv'))

            if new_row_count == previous_row_count:
                print("No new rows loaded, possibly reached the bottom")
                # Try one more aggressive scroll
                actions.move_to_element(scrollable_container)
                actions.click_and_hold().move_by_offset(0, 100).release().perform()
                time.sleep(5)
                final_row_count = len(driver.find_elements(By.CLASS_NAME, 'row-RdUXZpkv'))
                if final_row_count == new_row_count:
                    print("Reached the bottom of the table")
                    break

            previous_row_count = new_row_count
            scroll_count += 1

        print(f"Total rows loaded: {previous_row_count}")
        return previous_row_count

    except Exception as e:
        print(f"Error during table scrolling: {str(e)}")
        return 0

def scroll_table_3(driver, table_element):
    """Scroll through the table body to load all data, handling popups"""
    try:
        # First check and dismiss any initial popups
        popup_check_script = """
        return Boolean(
            document.querySelector('.tv-dialog__modal-wrap') || 
            document.querySelector('.tv-dialog__modal-container') ||
            document.querySelector('.js-dialog')
        );
        """
        
        has_popup = driver.execute_script(popup_check_script)
        if has_popup:
            dismiss_popups(driver)
            time.sleep(1)  # Wait for popup to clear
            
        # Find the scrollable container
        scroll_script = """
        return document.querySelector('.tableContainer-OuXcFHzP');
        """
        
        scrollable_container = driver.execute_script(scroll_script)
        if not scrollable_container:
            print("Could not find scrollable container")
            return 0
        
        scroll_count = 0
        max_scrolls = 50  # Increased max scrolls
        previous_row_count = 0
        
        while scroll_count < max_scrolls:
            # Get current row count
            current_row_count = driver.execute_script("""
                return document.querySelectorAll('.row-RdUXZpkv').length;
            """)
            
            print(f"Current row count: {current_row_count}")
            
            # Scroll using JavaScript
            driver.execute_script("""
                var container = arguments[0];
                container.scrollTop += container.clientHeight;
            """, scrollable_container)
            
            # Wait for new data to load
            time.sleep(2)
            
            # Check if we've reached the bottom
            new_row_count = driver.execute_script("""
                return document.querySelectorAll('.row-RdUXZpkv').length;
            """)
            
            if new_row_count == previous_row_count:
                print("No new rows loaded, possibly reached the bottom")
                # Try one more aggressive scroll
                driver.execute_script("""
                    var container = arguments[0];
                    container.scrollTop = container.scrollHeight;
                """, scrollable_container)
                time.sleep(3)
                final_row_count = driver.execute_script("""
                    return document.querySelectorAll('.row-RdUXZpkv').length;
                """)
                if final_row_count == new_row_count:
                    print("Reached the bottom of the table")
                    break
            
            previous_row_count = new_row_count
            scroll_count += 1
        
        print(f"Total rows loaded: {previous_row_count}")
        return previous_row_count
        
    except Exception as e:
        print(f"Error during table scrolling: {str(e)}")
        return 0

def scroll_table_old(driver, table_element):
    """Scroll through the table body to load all data, handling popups"""
    try:
        # First check and dismiss any initial popups
        popup_check_script = """
        return Boolean(
            document.querySelector('.tv-dialog__modal-wrap') || 
            document.querySelector('.tv-dialog__modal-container') ||
            document.querySelector('.js-dialog')
        );
        """
        
        has_popup = driver.execute_script(popup_check_script)
        if has_popup:
            dismiss_popups(driver)
            time.sleep(1)  # Wait for popup to clear
        
        # Find the scrollable table body
        scroll_script = """
        return document.querySelector('tbody[data-target="currencies.contentBox"]') || 
               document.querySelector('tbody[tabindex]') ||
               document.querySelector('div[data-role="grid"] tbody');
        """
        
        # Find the scrollable container
        scroll_script = """
        return document.querySelector('.tableContainer-OuXcFHzP');
        """
        
        scrollable_container = driver.execute_script(scroll_script)
        if not scrollable_container:
            print("Could not find scrollable container")
            return 0
            
        scroll_count = 0
        max_scrolls = 30
        previous_height = 0
        
        while scroll_count < max_scrolls:
            # Check for popup before each scroll
            # dismiss_popups(driver)
            
            # Get current scroll height
            current_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_container)
            
            print(f"Current scroll height: {current_height}")
            
            # Scroll the container
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable_container)
            
            # Wait for new data to load
            time.sleep(2)
            
            # Check for popup again after scroll
            # dismiss_popups(driver)
            
            # Check if we've reached the bottom
            new_height = driver.execute_script("return arguments[0].scrollHeight", scrollable_container)
            if new_height == previous_height:
                print("Reached the bottom of the table")
                break
            
            previous_height = new_height
            scroll_count += 1
            
        # Count the total number of rows
        rows_count = driver.execute_script("""
            return document.querySelectorAll('.row-RdUXZpkv').length;
        """)
        
        print(f"Total rows loaded: {rows_count}")
        return rows_count
        
    except Exception as e:
        print(f"Error during table scrolling: {str(e)}")
        return 0
        
def extract_table_data(table, driver):
    """Extract data from the table regardless of its structure"""
    
    js_extract_script = """
    function extractTableData(table) {
        // Get headers
        let headers = [];
        let headerElements = table.querySelectorAll('th, [role="columnheader"]');
        headerElements.forEach(header => {
            let text = header.textContent.trim();
            let field = header.getAttribute('data-field') || text;
            if (field && !headers.includes(field)) {
                headers.push(field);
            }
        });
        
        // Get rows
        let rows = [];
        let rowElements = table.querySelectorAll('tr, [role="row"]');
        rowElements.forEach(row => {
            if (!row.closest('thead')) {  // Skip header rows
                let cells = row.querySelectorAll('td, [role="gridcell"]');
                let rowData = {};
                cells.forEach((cell, index) => {
                    if (headers[index]) {
                        rowData[headers[index]] = cell.textContent.trim();
                    }
                });
                if (Object.keys(rowData).length > 0) {
                    rows.push(rowData);
                }
            }
        });
        
        return {headers: headers, rows: rows};
    }
    return extractTableData(arguments[0]);
    """
    
    try:
        # Extract data using JavaScript
        result = driver.execute_script(js_extract_script, table)
        
        if result and 'rows' in result and result['rows']:
            return pd.DataFrame(result['rows'])
        
        # Fallback: Manual extraction if JavaScript method fails
        rows = []
        for row in table.find_elements(By.CSS_SELECTOR, 'tr, [role="row"]'):
            if not row.find_elements(By.CSS_SELECTOR, 'th, [role="columnheader"]'):  # Skip header rows
                cells = row.find_elements(By.CSS_SELECTOR, 'td, [role="gridcell"]')
                row_data = {
                    'Symbol': '',
                    'Name': '',
                    'Price': '',
                    'Change_24h': '',
                    'Market_Cap': '',
                    'Volume': '',
                    'Category': ''
                }
                
                try:
                    # Extract symbol and name
                    name_element = row.find_element(By.CSS_SELECTOR, 'a[title]')
                    if name_element:
                        symbol_text = name_element.get_attribute('title')
                        row_data['Symbol'] = symbol_text.split(' − ')[0] if ' − ' in symbol_text else symbol_text
                        row_data['Name'] = symbol_text.split(' − ')[1] if ' − ' in symbol_text else ''
                except:
                    pass
                
                # Extract other fields
                for i, cell in enumerate(cells):
                    text = cell.text.strip()
                    if i == 2:  # Price
                        row_data['Price'] = text.split()[0] if text else ''
                    elif i == 3:  # Change
                        row_data['Change_24h'] = text
                    elif i == 4:  # Market Cap
                        row_data['Market_Cap'] = text.split()[0] if text else ''
                    elif i == 5:  # Volume
                        row_data['Volume'] = text.split()[0] if text else ''
                    elif i == 7:  # Category
                        row_data['Category'] = text
                
                rows.append(row_data)
        
        return pd.DataFrame(rows)
        
    except Exception as e:
        print(f"Error extracting table data: {str(e)}")
        return None

def scrape_tradingview_crypto():
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    
    driver = webdriver.Chrome()
    
    try:
        # 1. Navigate to the page
        driver.get('https://www.tradingview.com/crypto-coins-screener/')
        
        # Wait for the page to load
        wait = WebDriverWait(driver, 10)

        # 2. Get all header column sets (tabs)
        header_tabs = wait.until(EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, '#market-screener-header-columnset-tabs button[role="tab"]')
        ))
        
        all_data = {}
        
        # 3. Iterate through each tab
        for tab in header_tabs:
            try:
                tab_name = tab.find_element(By.CLASS_NAME, 'content-mf1FlhVw').text
                print(f"Processing tab: {tab_name}")
                if tab_name == "More":
                    continue

                # Click the tab
                driver.execute_script("arguments[0].click();", tab)
                time.sleep(2)  # Wait for table to update
                
                # Skip the fake "More" tab
                
                # Try to find the table
                table = find_table_element(driver)
                
                if not table:
                    print("Could not find the table element")
                    continue
                
                # Find the scrollable container for the table
                scrollable_element = table
                try:
                    # Try to find a more specific scrollable container
                    container = driver.find_element(By.CSS_SELECTOR, 'div[data-role="grid"] .table-wrapper')
                    if container:
                        scrollable_element = container
                except:
                    pass  # Use the table element if no specific container found
                
                # Scroll through the table to load all data
                total_rows = scroll_table(driver, scrollable_element)
                print(f"Total rows loaded for {tab_name}: {total_rows}")
                    
                    
                # Extract data from table
                df = extract_table_data(table, driver)
                
                all_data[tab_name] = df
            except Exception as e:
                print(f"Error during scraping: {str(e)}")
                return None
        return all_data
    except Exception as e:
        print(f"Error during scraping: {str(e)}")
        return None
    finally:
        driver.quit()

def save_to_mongodb(df, client):
    """Save the scraped data to MongoDB"""
    try:
        if df is not None and not df.empty:
            db = client["AIAggregator"]
            collection = db["TradingView_Crypto"]
            
            # Convert DataFrame to records
            records = df.to_dict('records')
            
            # Insert records
            if records:
                collection.insert_many(records)
                print(f"Saved {len(records)} records to MongoDB")
        else:
            print("No data to save to MongoDB")
            
    except Exception as e:
        print(f"Error saving to MongoDB: {str(e)}")

if __name__ == "__main__":
    try:
        data = scrape_tradingview_crypto()
        
        # Save each tab's data to a separate CSV file
        for tab_name, df in data.items():
            if df is not None and not df.empty:
                csv_filename = f"{tab_name.replace(' ', '_')}_tradingview_crypto.csv"
                df.to_csv(csv_filename, index=False)
                print(f"Saved data for tab '{tab_name}' to {csv_filename}")
        
        # if data is not None:
        #     print("\nSample of scraped data:")
        #     print(data.head())
            
        #     # Save to MongoDB
        #     import scraper_utils
        #     client = scraper_utils.connect_to_mongodb()
        #     save_to_mongodb(data, client)
            
            
    except Exception as e:
        print(f"Error in main execution: {str(e)}")