import asyncio
from playwright.async_api import async_playwright
import logging
from mss import mss
import os
import json
from datetime import datetime
import pyautogui
import re
import time
from pynput.keyboard import Controller, Key

# Set up logging
logger = logging.getLogger('BulkManualScraper')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
logger.addHandler(ch)

class BulkManualScraper:
    def __init__(self):
        self.base_url = "https://haynesmanualsallaccess.com/en-us/cars"
        self.progress_file = "scraping_progress.json"
        # Keep manuals_dir in the manual_scraper directory where it actually is
        self.manuals_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "downloaded_manuals"))
        self.current_vehicle = None  # Store current vehicle info
        self.max_save_retries = 3  # Maximum number of retries for saving
        self.consecutive_print_errors = 0  # Track consecutive print errors
        self.error_threshold = 5  # Number of consecutive errors before reset
        self.total_errors = 0  # Track total errors in current session
        self.session_error_threshold = 15  # Number of total errors before forcing reset
        
        # Initialize screen handling for PDF saving
        with mss() as sct:
            self.monitors = sct.monitors
            logger.info("Available monitors:")
            for i, m in enumerate(self.monitors):
                logger.info(f"Monitor {i}: {m}")
            
            # Use the primary monitor (usually index 1, index 0 is all monitors combined)
            self.target_monitor = self.monitors[1]
            logger.info(f"Using primary monitor: {self.target_monitor}")
            
            # Store monitor offset
            self.monitor_left = self.target_monitor["left"]
            self.monitor_top = self.target_monitor["top"]
            self.screen_width = self.target_monitor["width"]
            self.screen_height = self.target_monitor["height"]
            
            logger.info(f"Monitor dimensions: {self.screen_width}x{self.screen_height}")
            logger.info(f"Monitor offset: left={self.monitor_left}, top={self.monitor_top}")
            
        # Pre-calibrated coordinates for PDF saving
        self.print_coords = (-419, 920)  # Print button in Chrome dialog
        self.save_coords = (-582, 537)   # Save button coordinates
        self.cancel_print_coords = (-319, 920)  # Cancel button (100px right of print button)
        
        # Predefined list of valid makes
        self.valid_makes = [
            'Acura', 'Audi', 'BMW', 'Buick', 'Cadillac', 'Chevrolet', 
            'Chrysler', 'Dodge', 'Ford', 'GMC', 'Honda', 'Hyundai', 
            'Infiniti', 'Isuzu', 'Jaguar', 'Jeep', 'Kia', 'Lexus', 
            'Lincoln', 'Mazda', 'Mercedes-Benz', 'Mercury', 'Mini', 
            'Mitsubishi', 'Nissan', 'Oldsmobile', 'Pontiac', 'Ram', 
            'Saturn', 'Subaru', 'Toyota', 'Volkswagen', 'Volvo'
        ]
        
        # Create manuals directory
        os.makedirs(self.manuals_dir, exist_ok=True)
        
    def load_progress(self):
        """Load progress from JSON file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except:
                return {'last_make': None, 'last_model': None, 'completed': []}
        return {'last_make': None, 'last_model': None, 'completed': []}
        
    def save_progress(self, make, model=None):
        """Save progress to JSON file"""
        progress = self.load_progress()
        progress['last_make'] = make
        progress['last_model'] = model
        if model:  # Only add to completed if we finished a model
            progress['completed'].append({'make': make, 'model': model, 'timestamp': datetime.now().isoformat()})
        
        with open(self.progress_file, 'w') as f:
            json.dump(progress, f, indent=2)

    def get_make_url(self, make):
        """Convert make name to URL format"""
        # Convert make name to URL format (lowercase, dash-separated)
        url_make = make.lower().replace(' ', '-')
        return f"{self.base_url}/manufacturer/{url_make}"

    async def get_makes(self, page):
        """Get all vehicle makes"""
        # Ensure we're on the makes page
        if '/cars' not in page.url:
            await page.goto(self.base_url)
            await page.wait_for_load_state('networkidle')
        
        # Wait for user to confirm they're ready
        logger.info("\nPlease ensure you're logged in and on the make selection page")
        logger.info("Press Enter when ready (or 'q' to quit):")
        
        response = input()
        if response.lower() == 'q':
            return []
            
        # Wait for the content to load
        await asyncio.sleep(2)
        
        try:
            # Return the predefined list of makes
            logger.info(f"Using predefined list of {len(self.valid_makes)} makes: {self.valid_makes}")
            return self.valid_makes
            
        except Exception as e:
            logger.error(f"Error getting makes: {str(e)}")
            return []

    async def get_models(self, page, make):
        """Get all models for a specific make"""
        try:
            logger.info(f"\nProcessing make: {make}")
            
            # First go to the main cars page
            await page.goto(self.base_url)
            await page.wait_for_load_state('networkidle')
            await asyncio.sleep(1)
            
            # Click on the make's link
            make_selector = f"text={make}"
            logger.info(f"Looking for make link with selector: {make_selector}")
            
            try:
                # Click the make link
                await page.click(make_selector)
                await page.wait_for_load_state('networkidle')
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error clicking make link: {str(e)}")
                return []

            # Get all links on the page
            links = await page.query_selector_all('a')
            model_links = []
            
            import re
            # Pattern to match: "Model Name (YYYY - YYYY)" or "Model Name (YYYY-YYYY)"
            pattern = re.compile(rf"{make}\s+[\w\s-]+\(\d{{4}}(?:\s*-\s*|\s*–\s*)\d{{4}}\)")
            
            for link in links:
                try:
                    text = await link.text_content()
                    
                    # Skip if no text
                    if not text:
                        continue
                    
                    # Clean up the text (remove extra whitespace)
                    text = ' '.join(text.split())
                    
                    # Check if the text matches our pattern
                    if pattern.match(text):
                        # Verify this is a clickable link
                        if await link.is_visible() and await link.is_enabled():
                            logger.info(f"Found valid model: {text}")
                            model_links.append({
                                'name': text,
                                'element': link
                            })
                        else:
                            logger.warning(f"Found matching but non-clickable model: {text}")
                    else:
                        logger.debug(f"Skipping non-matching link: {text}")
                        
                except Exception as e:
                    logger.error(f"Error processing link: {str(e)}")
                    continue
            
            if not model_links:
                logger.warning(f"No valid models found for {make}")
                
            return model_links
            
        except Exception as e:
            logger.error(f"Error getting models for {make}: {str(e)}")
            return []

    async def get_model_names(self, page):
        """Get model names from the current page"""
        try:
            # Try different selectors for models, ordered from most specific to least
            selectors = [
                '.vehicle-model',  # Specific vehicle model class
                '.model-name',     # Model name class
                '.models a',       # Links within models container
                '.vehicle-list a'  # Links within vehicle list
            ]
            
            models = []
            for selector in selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    if elements:
                        logger.info(f"Found elements with selector: {selector}")
                        for element in elements:
                            try:
                                text = await element.text_content()
                                href = await element.get_attribute('href')
                                
                                # Skip if it's a techbook
                                if text and 'techbook' in text.lower():
                                    continue
                                    
                                # Skip if it's a generic manual
                                if href and '/MOL' in href:
                                    continue
                                
                                if text and text.strip():
                                    # Skip models older than 1990
                                    text = text.strip()
                                    years = []
                                    # Extract all years from the text
                                    for word in text.replace('-', ' ').split():
                                        try:
                                            year = int(word)
                                            if 1900 <= year <= 2100:  # Sanity check for valid years
                                                years.append(year)
                                        except ValueError:
                                            continue
                                    
                                    # Skip if any year is before 1990
                                    if years and any(year < 1990 for year in years):
                                        logger.info(f"Skipping pre-1990 model: {text}")
                                        continue
                                    
                                    models.append(text.strip())
                            except:
                                continue
                except:
                    continue
            
            # Remove duplicates while preserving order
            unique_models = []
            for model in models:
                if model not in unique_models:
                    unique_models.append(model)
            
            logger.info(f"Found {len(unique_models)} models: {unique_models}")
            return unique_models
            
        except Exception as e:
            logger.error(f"Error getting model names: {str(e)}")
            return []

    async def click_text_element(self, page, target_text, description="element"):
        """Helper function to click an element by its text content"""
        try:
            # Get all elements on the page
            elements = await page.query_selector_all('*')
            clicked = False
            
            for element in elements:
                try:
                    text = await element.text_content()
                    text = text.strip()
                    
                    # Check for exact text match
                    if text.lower() == target_text.lower():
                        logger.info(f"Found {description} text: {text}")
                        # Make sure the element is visible and clickable
                        if await element.is_visible() and await element.is_enabled():
                            await element.click()
                            clicked = True
                            logger.info(f"Clicked {description}: {text}")
                            break
                except:
                    continue
            
            if not clicked:
                logger.error(f"Could not find clickable {description}")
                # Log all visible text elements to help debug
                logger.info(f"Visible text elements while looking for {description}:")
                for element in elements:
                    try:
                        if await element.is_visible():
                            text = await element.text_content()
                            text = text.strip()
                            if text:
                                logger.info(f"  - {text}")
                    except:
                        continue
                return False
            
            await page.wait_for_load_state('networkidle')
            await asyncio.sleep(1)
            return True
            
        except Exception as e:
            logger.error(f"Error clicking {description}: {str(e)}")
            return False

    async def get_all_menu_items(self, frame):
        """Get all menu items and determine which are visible"""
        menu_items = []
        items = await frame.query_selector_all('.menu__link')
        for item in items:
            try:
                is_visible = await item.is_visible()
                if is_visible:
                    text = await item.text_content()
                    text = text.strip()
                    if text:  # Only add non-empty text items
                        is_main = await item.get_attribute('class') == 'menu__link menu__link--level-0'
                        menu_items.append({
                            'text': text,
                            'element': item,
                            'is_main': is_main
                        })
            except Exception as e:
                logger.error(f"Error processing menu item: {str(e)}")
        return menu_items

    async def click_menu_item(self, frame, menu_text):
        """Click a menu item and wait for it to load"""
        try:
            logger.info(f"Clicking menu item: {menu_text}")
            
            # Try different selectors to find the menu item
            selectors = [
                f"text='{menu_text}'",
                f"a:has-text('{menu_text}')",
                f".menu__link:has-text('{menu_text}')"
            ]
            
            # First try to locate the element
            menu_item = None
            for selector in selectors:
                try:
                    # First check if element exists
                    elements = await frame.query_selector_all(selector)
                    if not elements:
                        logger.info(f"No elements found for selector {selector}")
                        continue
                        
                    # Find first visible element
                    for element in elements:
                        try:
                            if await element.is_visible():
                                menu_item = element
                                logger.info(f"Found visible element with selector {selector}")
                                break
                        except Exception as e:
                            continue
                            
                    if menu_item:
                        break
                        
                except Exception as e:
                    logger.error(f"Error checking selector {selector}: {str(e)}")
                    continue
            
            if not menu_item:
                logger.error(f"No visible menu item found for: {menu_text}")
                return False
                
            # Now that we found the element, try to click it
            try:
                # Scroll into view if needed
                await menu_item.scroll_into_view_if_needed()
                
                # Simple visibility check before click
                if await menu_item.is_visible():
                    await menu_item.click()
                    await asyncio.sleep(0.5)  # Brief pause after click
                    return True
                else:
                    logger.error("Element not visible before click")
                    return False
                    
            except Exception as e:
                logger.error(f"Error during click operation: {str(e)}")
                return False
            
        except Exception as e:
            logger.error(f"Error clicking menu item {menu_text}: {str(e)}")
            return False

    async def get_submenu_items(self, frame, main_menu_items):
        """Get all submenu items after clicking a main menu item"""
        try:
            # Wait for navigation and content to load
            await self.wait_for_navigation(frame)
            await asyncio.sleep(1)
            
            # Try both old and new menu structures
            all_items = []
            
            # First try the old structure (.menu__link:not(.menu__link--level-0))
            try:
                old_items = await frame.query_selector_all('.menu__link:not(.menu__link--level-0)')
                if old_items:
                    logger.info(f"Found {len(old_items)} items with old menu structure")
                    all_items.extend(old_items)
            except Exception as e:
                logger.error(f"Error getting old menu items: {str(e)}")
            
            # Then try the new structure (.subMenuOpened .menu__link)
            try:
                new_items = await frame.query_selector_all('.subMenuOpened .menu__link')
                if new_items:
                    logger.info(f"Found {len(new_items)} items with new menu structure")
                    all_items.extend(new_items)
            except Exception as e:
                logger.error(f"Error getting new menu items: {str(e)}")
            
            # If neither worked, try the generic submenu-item class
            if not all_items:
                try:
                    submenu_items = await frame.query_selector_all('.submenu-item')
                    if submenu_items:
                        logger.info(f"Found {len(submenu_items)} items with submenu-item class")
                        all_items.extend(submenu_items)
                except Exception as e:
                    logger.error(f"Error getting submenu items: {str(e)}")
            
            if not all_items:
                logger.error("No submenu items found with any selector")
                return []
            
            # Clean up main menu items for comparison
            clean_main_menu_items = set(' '.join(item.split()) for item in main_menu_items)
            logger.info(f"Main menu items to filter out: {clean_main_menu_items}")
            
            # Process all found items
            submenu_items = []
            for item in all_items:
                try:
                    is_visible = await item.is_visible()
                    if is_visible:
                        text = await item.text_content()
                        text = ' '.join(text.strip().split())  # Normalize whitespace
                        text = ''.join(c for c in text if c.isprintable())  # Remove non-printable chars
                        
                        # Skip if empty, too short, or is a main menu item
                        if not text or len(text) < 2 or text in clean_main_menu_items:
                            continue
                        
                        # Skip items that start with numbers outside our expected range (0-99)
                        if text[0].isdigit():
                            try:
                                section_num = int(text.split()[0])
                                if section_num < 0 or section_num > 99:
                                    logger.info(f"Skipping item outside valid range (0-99): {text}")
                                    continue
                            except ValueError:
                                pass  # Not a valid number, continue processing
                        
                        # Store both text and element reference
                        if not any(s['text'] == text for s in submenu_items):  # Avoid duplicates
                            logger.info(f"Found valid submenu item: {text}")
                            submenu_items.append({
                                'text': text,
                                'element': item
                            })
                except Exception as e:
                    continue
            
            if not submenu_items:
                logger.error("No valid submenu items found")
            else:
                logger.info(f"Successfully processed {len(submenu_items)} submenu items")
                logger.info(f"Found {len(submenu_items)} submenu items")
            
            return submenu_items
            
        except Exception as e:
            logger.error(f"Error getting submenu items: {str(e)}")
            return []

    async def get_visible_menu_items(self, frame):
        """Get all currently visible menu items"""
        try:
            # Get all menu links
            items = await frame.query_selector_all('.menu__link')
            visible_items = []
            
            # First find the start (General Information) and end (Wiring Diagrams) indices
            start_idx = None
            end_idx = None
            temp_items = []
            
            for i, item in enumerate(items):
                try:
                    is_visible = await item.is_visible()
                    if is_visible:
                        text = await item.text_content()
                        text = ' '.join(text.strip().split())  # Normalize whitespace
                        text = ''.join(c for c in text if c.isprintable())  # Remove non-printable chars
                        
                        if text:
                            temp_items.append((i, text, item))
                            # Look for variants of General Information
                            if any(text.lower().endswith(x) for x in ['general information', 'general info', '0 general information']):
                                start_idx = i
                            # Look for variants of Wiring Diagrams
                            elif any(text.lower().endswith(x) for x in ['wiring diagram', 'wiring diagrams', 'wiring']):
                                end_idx = i
                except:
                    continue
            
            if start_idx is not None and end_idx is not None:
                # Get all items between start and end (inclusive)
                main_items = [item for i, text, item in temp_items if start_idx <= i <= end_idx]
                main_texts = [text for i, text, item in temp_items if start_idx <= i <= end_idx]
                
                logger.info(f"Found main menu items from '{main_texts[0]}' to '{main_texts[-1]}'")
                
                for item, text in zip(main_items, main_texts):
                    visible_items.append({
                        'text': text,
                        'element': item,
                        'is_main': True
                    })
            else:
                logger.warning("Could not find boundary items, falling back to class-based detection")
                # Fallback to class-based detection
                for item in items:
                    try:
                        is_visible = await item.is_visible()
                        if is_visible:
                            text = await item.text_content()
                            text = ' '.join(text.strip().split())
                            text = ''.join(c for c in text if c.isprintable())
                            
                            if text and len(text) > 1:
                                class_attr = await item.get_attribute('class')
                                if 'menu__link--level-0' in (class_attr or ''):
                                    visible_items.append({
                                        'text': text,
                                        'element': item,
                                        'is_main': True
                                    })
                    except:
                        continue
            
            return visible_items
            
        except Exception as e:
            logger.error(f"Error getting visible menu items: {str(e)}")
            return []

    async def get_submenu_items(self, frame, main_menu_items):
        """Get all submenu items after clicking a main menu item"""
        try:
            submenu_texts = []
            
            # Wait for submenu to be visible
            await self.wait_for_navigation(frame)
            await asyncio.sleep(1)
            
            # Get all menu items
            items = await frame.query_selector_all('.menu__link:not(.menu__link--level-0)')
            logger.info(f"Found {len(items)} potential submenu items")
            
            # Clean up main menu items for comparison
            clean_main_menu_items = set(' '.join(item.split()) for item in main_menu_items)
            logger.info(f"Main menu items to filter out: {clean_main_menu_items}")
            
            for item in items:
                try:
                    is_visible = await item.is_visible()
                    if is_visible:
                        text = await item.text_content()
                        text = ' '.join(text.strip().split())  # Normalize whitespace
                        text = ''.join(c for c in text if c.isprintable())  # Remove non-printable chars
                        
                        # Skip if empty, too short, or is a main menu item
                        if not text or len(text) < 2 or text in clean_main_menu_items:
                            continue
                        
                        # Skip items that start with numbers outside our expected range (0-99)
                        if text[0].isdigit():
                            try:
                                section_num = int(text.split()[0])
                                if section_num < 0 or section_num > 99:
                                    logger.info(f"Skipping item outside valid range (0-99): {text}")
                                    continue
                            except ValueError:
                                pass  # Not a valid number, continue processing
                        
                        if text not in submenu_texts:  # Avoid duplicates
                            logger.info(f"Found valid submenu item: {text}")
                            submenu_texts.append(text)
                            
                except Exception as e:
                    continue
            
            if not submenu_texts:
                logger.error("No submenu items found")
            else:
                logger.info(f"Successfully processed {len(submenu_texts)} submenu items")
            
            return submenu_texts
            
        except Exception as e:
            logger.error(f"Error getting submenu items: {str(e)}")
            return []

    async def click_submenu_item(self, frame, submenu_text):
        """Click a submenu item specifically"""
        logger.info(f"Attempting to click submenu item: {submenu_text}")
        start_time = time.time()
        last_action_time = start_time
        
        # Define cleanup sequences to try
        cleanup_sequences = [
            # Sequence 1: Handle Print Dialog
            [
                (self.cancel_print_dialog, "Cancel Print Dialog"),
                (lambda: asyncio.sleep(1), "Wait after print dialog cancel"),
                (lambda: pyautogui.press('esc'), "ESC key after print dialog"),
                (lambda: asyncio.sleep(1), "Wait after ESC"),
            ],
            # Sequence 2: Handle Save Dialog
            [
                (lambda: pyautogui.press('esc'), "ESC key for Save Dialog"),
                (lambda: asyncio.sleep(1), "Wait after ESC"),
                (lambda: self.click_coordinates(*self.cancel_print_coords), "Click Cancel in Save Dialog"),
                (lambda: asyncio.sleep(1), "Wait after cancel click"),
            ],

            # Sequence 3: Handle File Exists Dialog
            [
                (lambda: pyautogui.press('n'), "Press 'N' for No on file exists"),
                (lambda: asyncio.sleep(1), "Wait after No"),
                (lambda: pyautogui.press('esc'), "ESC after file exists"),
                (lambda: asyncio.sleep(1), "Wait after ESC"),
            ],
            # Sequence 4: Thorough cleanup of all possible dialogs
            [
                (self.cancel_print_dialog, "Initial Print Dialog Cancel"),
                (lambda: asyncio.sleep(1), "Wait 1"),
                (lambda: pyautogui.press('esc'), "ESC for Save Dialog"),
                (lambda: asyncio.sleep(1), "Wait 2"),
                (lambda: pyautogui.press('n'), "No for File Exists"),
                (lambda: asyncio.sleep(1), "Wait 3"),
                (lambda: self.click_coordinates(*self.cancel_print_coords), "Click Cancel Button"),
                (lambda: asyncio.sleep(1), "Wait 4"),
                (lambda: pyautogui.press('esc'), "Final ESC"),
                (lambda: asyncio.sleep(2), "Final Wait"),
            ],
        ]

        while time.time() - start_time < 120:  # 2 minutes max total time
            current_time = time.time()
            
            # First try clicking without cleanup
            if await self.try_click_submenu(frame, submenu_text):
                return True
            
            # If no progress in 15 seconds (reduced from 30), try cleanup sequences
            if current_time - last_action_time > 15:
                logger.warning(f"Been stuck for {int(current_time - last_action_time)}s, trying cleanup sequences...")
                
                # Try each cleanup sequence
                for sequence_num, sequence in enumerate(cleanup_sequences, 1):
                    logger.info(f"Trying cleanup sequence {sequence_num} of {len(cleanup_sequences)}...")
                    
                    # Execute each step in the sequence
                    for action, description in sequence:
                        try:
                            logger.info(f"Executing cleanup step: {description}")
                            if asyncio.iscoroutinefunction(action):
                                await action()
                            else:
                                action()
                        except Exception as e:
                            logger.error(f"Error during cleanup ({description}): {str(e)}")
                            continue
                    
                    # After each sequence, try clicking again
                    logger.info(f"Attempting click after cleanup sequence {sequence_num}")
                    if await self.try_click_submenu(frame, submenu_text):
                        return True
                    
                    # Short wait before trying next sequence
                    await asyncio.sleep(2)
                
                # Update last_action_time after trying all sequences
                last_action_time = time.time()
            
            await asyncio.sleep(1)
        
        logger.error(f"Failed to click submenu item after {int(time.time() - start_time)}s: {submenu_text}")
        return False

    async def try_click_submenu(self, frame, submenu_text):
        """Helper method to try clicking a submenu item with various selectors"""
        for selector in ['.menu__link:not(.menu__link--level-0)', '.submenu-item', 'a']:
            try:
                elements = await frame.query_selector_all(selector)
                
                for el in elements:
                    try:
                        text = await el.text_content()
                        text = ' '.join(text.strip().split())
                        
                        if text == submenu_text:
                            if await el.is_visible() and await el.is_enabled():
                                logger.info(f"Found visible element with selector {selector}")
                                try:
                                    # Use shorter 15 second timeout for click
                                    await el.click(timeout=15000)
                                    logger.info("Successfully clicked submenu item")
                                    # Wait after clicking to ensure navigation starts
                                    await asyncio.sleep(3)
                                    return True
                                except Exception as click_error:
                                    logger.error(f"Click failed: {str(click_error)}")
                                    # Immediately run cleanup procedures after click failure
                                    logger.info("Running cleanup after click failure...")
                                    await self.cancel_print_dialog()
                                    await asyncio.sleep(1)
                                    pyautogui.press('esc')
                                    await asyncio.sleep(1)
                                    await self.click_coordinates(*self.cancel_print_coords)
                                    await asyncio.sleep(1)
                    except Exception as e:
                        continue
            except Exception as e:
                continue
        return False

    async def process_model(self, frame):
        """Process all sections of a manual"""
        try:
            # Get all visible menu items
            visible_items = await self.get_visible_menu_items(frame)
            if not visible_items:
                logger.error("Could not find any menu items")
                return False
            
            # Convert to clean text list for main menu items
            main_menu_items = [item['text'] for item in visible_items]
            logger.info(f"Found main menu items from '{main_menu_items[0]}' to '{main_menu_items[-1]}'")
            logger.info(f"Found {len(visible_items)} visible main menu items")
            logger.info(f"Main menu items: {main_menu_items}")
            
            # Process each main menu item
            for menu_item in visible_items:
                main_menu_text = menu_item['text']
                
                # Skip Reference section
                if 'Reference' in main_menu_text:
                    logger.info(f"Skipping Reference section: {main_menu_text}")
                    continue
                
                # Click main menu item and wait for navigation
                logger.info(f"Clicking menu item: {main_menu_text}")
                if not await self.click_menu_item(frame, main_menu_text):
                    logger.error(f"Could not click {main_menu_text} menu")
                    continue
                
                # Wait for menu to expand and content to load
                await asyncio.sleep(3)  # Increased wait time
                try:
                    # Wait for either the old submenu-item class or the new menu__link class within subMenuOpened
                    await frame.wait_for_selector('.submenu-item, .subMenuOpened .menu__link', timeout=5000)
                except Exception as e:
                    logger.error(f"Timeout waiting for submenu items to load: {str(e)}")
                    continue
                
                # Get all submenu items
                submenu_items = await self.get_submenu_items(frame, main_menu_items)
                if not submenu_items:
                    logger.error(f"No submenu items found for {main_menu_text}")
                    continue
                
                logger.info(f"Found {len(submenu_items)} submenu items")
                
                # Process all submenu items
                processed_count = 0
                for submenu_item in submenu_items:
                    if isinstance(submenu_item, dict):
                        clean_submenu = ' '.join(submenu_item['text'].strip().split())
                        element = submenu_item.get('element')
                    else:
                        clean_submenu = ' '.join(submenu_item.strip().split())
                        element = None
                    
                    # Skip if already exists
                    save_path = self.create_save_path(main_menu_text, clean_submenu)
                    if os.path.exists(save_path):
                        logger.info(f"Skipping existing file: {clean_submenu}")
                        processed_count += 1
                        continue

                    logger.info(f"Processing section: {clean_submenu}")

                    click_success = False
                    
                    try:
                        if element is not None:
                            # Try stored element with timeout
                            try:
                                click_future = element.click()
                                await asyncio.wait_for(click_future, timeout=30)
                                await self.wait_for_navigation(frame)
                                click_success = True
                            except asyncio.TimeoutError:
                                logger.warning("Stored element click timed out, trying submenu click...")
                            except Exception as e:
                                logger.error(f"Error clicking stored element: {str(e)}")
                        
                        # If stored element failed or wasn't available, try submenu click
                        if not click_success:
                            click_success = await self.click_submenu_item(frame, clean_submenu)
                        
                        # Only proceed with PDF saving if click was successful
                        if click_success:
                            if await self.save_as_pdf(frame, clean_submenu, main_menu_text):
                                logger.info(f"✓ Saved section: {clean_submenu}")
                                processed_count += 1
                                
                                # Re-click main menu item to restore submenu context
                                logger.info(f"Restoring menu context for: {main_menu_text}")
                                if not await self.click_menu_item(frame, main_menu_text):
                                    logger.error(f"Could not restore menu context for {main_menu_text}")
                                    continue
                                    
                                # Wait for menu to expand and content to load
                                await asyncio.sleep(2)
                                try:
                                    await frame.wait_for_selector('.submenu-item, .subMenuOpened .menu__link', timeout=5000)
                                except Exception as e:
                                    logger.error(f"Timeout waiting for submenu items to reload: {str(e)}")
                                    continue
                            else:
                                logger.error(f"Failed to save section: {clean_submenu}")
                        else:
                            logger.error(f"Could not click section: {clean_submenu}")
                            
                    except Exception as e:
                        logger.error(f"Error processing submenu item: {str(e)}")
                    
                    await asyncio.sleep(1)  # Wait between submenu items
            
            logger.info(f"Successfully processed {processed_count} submenu items")
            
            return True
            
        except Exception as e:
            logger.error(f"Error in process_model: {str(e)}")
            return False

    async def process_page_content(self, frame, main_menu_text, submenu_text):
        """Process and save the current page content as PDF"""
        try:
            logger.info(f"Processing page content for {main_menu_text} -> {submenu_text}")
            
            # Save the page as PDF
            success = await self.save_as_pdf(frame, submenu_text, main_menu_text)
            if success:
                logger.info(f"Successfully saved PDF for {submenu_text}")
                return True
            else:
                logger.error(f"Failed to save PDF for {submenu_text}")
                return False
                
        except Exception as e:
            logger.error(f"Error processing page content: {str(e)}")
            return False

    async def try_save_pdf(self, page, save_path):
        """Try to save PDF with current settings"""
        try:
            await page.pdf({'path': save_path})
            return True
        except Exception as e:
            logger.error(f"Error in try_save_pdf: {str(e)}")
            return False

    async def save_as_pdf(self, frame, section_title, main_menu_section):
        """Save the current page as PDF using hardcoded coordinates"""
        try:
            # Create proper save path
            save_path = self.create_save_path(main_menu_section, section_title)
            if not save_path:
                return False

            retry_count = 0
            while retry_count < self.max_save_retries:
                try:
                    # Ensure cleanup before each attempt (except first)
                    if retry_count > 0:
                        logger.info("Cleaning up before retry...")
                        await self.cancel_print_dialog()
                        await asyncio.sleep(2)  # Give time for cleanup
                        await self.cancel_print_dialog()  # Double check
                        await asyncio.sleep(1)

                    # 1. Locate and click webpage print button
                    logger.info("1. Clicking webpage print button...")
                    try:
                        # Simple frame click with 2 second timeout
                        await frame.click('button:has-text("Print")', timeout=2000)
                    except:
                        pass  # Continue regardless of whether click worked
                    await asyncio.sleep(3)  # Wait longer for print dialog
                    
                    # 2. Click system print dialog button
                    logger.info("2. Clicking system print dialog button...")
                    try:
                        abs_print_x, abs_print_y = self.get_absolute_coordinates(self.print_coords[0], self.print_coords[1])
                        pyautogui.moveTo(abs_print_x, abs_print_y, duration=0.2)
                        pyautogui.click()
                        await asyncio.sleep(1)  # Wait for dialog
                    except Exception as e:
                        logger.error(f"Error clicking print dialog: {str(e)}")
                        await self.cancel_print_dialog()
                        continue
                    
                    # 3. Type save path
                    logger.info("3. Typing save path...")
                    try:
                        pyautogui.write(save_path)
                        await asyncio.sleep(1)  # Wait after typing
                    except Exception as e:
                        logger.error(f"Error typing save path: {str(e)}")
                        await self.cancel_print_dialog()
                        continue
                    
                    # 4. Click save button
                    logger.info("4. Moving to save button...")
                    try:
                        abs_save_x, abs_save_y = self.get_absolute_coordinates(self.save_coords[0], self.save_coords[1])
                        pyautogui.moveTo(abs_save_x, abs_save_y, duration=0.2)
                        pyautogui.click()
                        await asyncio.sleep(1)  # Wait for save
                    except Exception as e:
                        logger.error(f"Error clicking save button: {str(e)}")
                        await self.cancel_print_dialog()
                        continue
                    
                    # 5. Verify the save
                    if await self.verify_pdf_save(save_path):
                        logger.info(f"✓ Saved PDF: {os.path.basename(save_path)}")
                        self.consecutive_print_errors = 0  # Reset error counter on success
                        return True
                    
                    # If verification failed, ensure cleanup before retry
                    logger.warning("PDF verification failed, cleaning up before retry...")
                    await self.cancel_print_dialog()
                    await asyncio.sleep(2)
                    await self.cancel_print_dialog()
                    
                    retry_count += 1
                    self.consecutive_print_errors += 1
                    self.total_errors += 1
                
                except Exception as e:
                    logger.error(f"Error during save attempt {retry_count + 1}: {str(e)}")
                    retry_count += 1
                    self.consecutive_print_errors += 1
                    self.total_errors += 1
                    # Ensure cleanup after error
                    await self.cancel_print_dialog()
                    await asyncio.sleep(2)
                    await self.cancel_print_dialog()
                    await asyncio.sleep(1)
            
            logger.error(f"Failed to save PDF after {self.max_save_retries} attempts")
            return False
        
        except Exception as e:
            logger.error(f"Error saving PDF: {str(e)}")
            await self.cancel_print_dialog()  # Final cleanup
            return False

    async def cancel_print_dialog(self):
        """Cancel the print dialog and return to normal state"""
        try:
            keyboard = Controller()
            
            # Log what we're doing more accurately
            logger.info("Attempting to clear any blocking dialogs...")
            
            # Define keyboard action sequences
            async def execute_keyboard_sequence(actions):
                for action in actions:
                    if isinstance(action, tuple):
                        key, press = action
                        if press:
                            keyboard.press(key)
                        else:
                            keyboard.release(key)
                    elif isinstance(action, float):
                        await asyncio.sleep(action)
            
            # Define different key sequences to try
            sequences = [
                # Tab and Enter sequence for file exists dialog
                [
                    (Key.tab, True),
                    (Key.tab, False),
                    0.2,
                    (Key.enter, True),
                    (Key.enter, False)
                ],
                # Escape sequence for general dialogs
                [
                    (Key.esc, True),
                    (Key.esc, False)
                ],
                # Alt+N sequence for "No" button
                [
                    (Key.alt, True),
                    ('n', True),
                    ('n', False),
                    (Key.alt, False)
                ],
                # Direct Enter for simple dialogs
                [
                    (Key.enter, True),
                    (Key.enter, False)
                ]
            ]
            
            # Try each sequence
            for sequence in sequences:
                await execute_keyboard_sequence(sequence)
                await asyncio.sleep(0.5)  # Reduced wait between attempts
                
            logger.info("Completed dialog handling attempts")
            
        except Exception as e:
            logger.error(f"Error in cancel_print_dialog: {str(e)}")
            raise

    async def save_pdf(self, page, save_path, retries=3):
        """Save the current page as PDF"""
        try:
            # Store current page and save path for dialog cancellation
            self.current_page = page
            self.current_save_path = save_path
            
            consecutive_errors = 0
            max_consecutive_errors = 2
            
            while retries > 0:
                try:
                    success = await self.try_save_pdf(page, save_path)
                    if success:
                        consecutive_errors = 0
                        return True
                    
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        logger.info("Multiple errors detected, attempting to cancel dialogs...")
                        if await self.cancel_print_dialog():
                            # Successfully cancelled dialogs, try save again
                            success = await self.try_save_pdf(page, save_path)
                            if success:
                                return True
                        
                except Exception as e:
                    logger.error(f"Error during PDF save attempt: {str(e)}")
                    consecutive_errors += 1
                
                retries -= 1
                if retries > 0:
                    await asyncio.sleep(1)
            
            return False
            
        except Exception as e:
            logger.error(f"Error in save_pdf: {str(e)}")
            return False

    async def verify_pdf_save(self, pdf_path, wait_times=[2, 4, 10]):
        """Verify PDF was saved successfully with progressive wait times"""
        for wait_time in wait_times:
            logger.info(f"Waiting {wait_time} seconds for PDF to save...")
            await asyncio.sleep(wait_time)
            
            if os.path.exists(pdf_path):
                try:
                    # Check if file is empty
                    if os.path.getsize(pdf_path) == 0:
                        logger.info(f"PDF file is empty after {wait_time}s wait")
                        continue
                        
                    # Try to open the PDF to verify it's valid
                    with open(pdf_path, 'rb') as f:
                        content = f.read()
                        if content.startswith(b'%PDF'):
                            logger.info(f"PDF verified: {os.path.basename(pdf_path)} ({os.path.getsize(pdf_path)} bytes)")
                            return True
                except Exception as e:
                    logger.error(f"Error verifying PDF: {str(e)}")
            else:
                logger.info(f"PDF file not found after {wait_time}s wait")
                
            # If we get here, verification failed for this wait time
            # Clean up any dialogs before next attempt
            await self.cancel_print_dialog()
            await asyncio.sleep(1)
        
        # If we get here, all verification attempts failed
        logger.error(f"Failed to verify PDF after all retries: {os.path.basename(pdf_path)}")
        # Do a thorough cleanup before returning
        await self.cancel_print_dialog()
        await asyncio.sleep(2)
        await self.cancel_print_dialog()
        return False

    def get_absolute_coordinates(self, x, y):
        """Convert coordinates relative to target monitor to absolute screen coordinates"""
        abs_x = self.monitor_left + x
        abs_y = self.monitor_top + y
        logger.info(f"Converting coordinates: ({x}, {y}) -> ({abs_x}, {abs_y})")
        return abs_x, abs_y

    async def wait_for_navigation(self, frame):
        """Wait for page to finish loading"""
        try:
            await frame.wait_for_load_state('domcontentloaded', timeout=30000)
            await frame.wait_for_load_state('networkidle', timeout=30000)
            await asyncio.sleep(1)  # Additional wait to ensure page is stable
        except Exception as e:
            logger.error(f"Navigation wait error: {str(e)}")

    async def reset_to_makes_page(self, page, error_type="consecutive"):
        """Reset the browser state and return to makes page"""
        try:
            logger.warning(f"\n{'='*50}")
            logger.warning(f"Initiating fail-safe reset due to {error_type} errors")
            logger.warning(f"{'='*50}\n")
            
            # Close any open dialogs
            pyautogui.press('escape')
            await asyncio.sleep(1)
            pyautogui.press('escape')
            await asyncio.sleep(1)
            
            # Try to click cancel on print dialog if it exists
            try:
                abs_cancel_x, abs_cancel_y = self.get_absolute_coordinates(self.cancel_print_coords[0], self.cancel_print_coords[1])
                pyautogui.moveTo(abs_cancel_x, abs_cancel_y, duration=0.2)
                pyautogui.click()
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Error clicking cancel button: {str(e)}")
            
            # Navigate back to makes page
            try:
                await page.goto(self.base_url, wait_until='networkidle', timeout=30000)
                await asyncio.sleep(2)
                return True
            except Exception as e:
                logger.error(f"Failed to navigate to base URL: {str(e)}")
                return False
            
        except Exception as e:
            logger.error(f"Critical error in reset_to_makes_page: {str(e)}")
            return False

    async def reset_to_make_page(self, page, make_url):
        """Reset back to the make's main page"""
        try:
            # First try going directly to the make URL
            try:
                await page.goto(make_url, wait_until='networkidle', timeout=10000)
                await asyncio.sleep(2)
                return True
            except:
                pass

            # If that fails, try going to base URL first
            try:
                await page.goto(self.base_url, wait_until='networkidle', timeout=10000)
                await asyncio.sleep(1)
                await page.goto(make_url, wait_until='networkidle', timeout=10000)
                await asyncio.sleep(2)
                return True
            except Exception as e:
                logger.error(f"Failed to reset to make page: {str(e)}")
                return False

        except Exception as e:
            logger.error(f"Error in reset_to_make_page: {str(e)}")
            return False

    def get_folder_name(self, model_name):
        """Convert website model name to folder name format"""
        try:
            # Extract model name and year range
            match = re.search(r'(.*?)\s*\((\d{4})\s*-\s*(\d{4})\)', model_name)
            if match:
                base_name, year1, year2 = match.groups()
                # Remove extra spaces and combine with years
                folder_name = f"{base_name.strip()} {year1}-{year2}"
                return folder_name
            return model_name  # Return as is if pattern doesn't match
        except Exception as e:
            logger.error(f"Error converting model name to folder name: {str(e)}")
            return model_name

    async def click_view_manual_button(self, page):
        """Click the View Online Manual button"""
        try:
            # Wait for network to be idle and a bit more time for page to stabilize
            await page.wait_for_load_state('networkidle')
            await asyncio.sleep(3)

            # Wait for the button to be visible and stable
            button_selector = 'text="View online manual"'
            await page.wait_for_selector(button_selector, state='visible', timeout=10000)
            
            # Additional wait to ensure page is fully interactive
            await asyncio.sleep(2)
            
            # Find all matching elements
            elements = await page.query_selector_all(button_selector)
            
            if not elements:
                logger.error("View online manual button not found")
                return False
                
            # Get the first visible button
            for element in elements:
                if await element.is_visible():
                    # Log before clicking
                    logger.info("Found View Online Manual button, attempting to click...")
                    
                    # Click with retry logic
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            await element.click()
                            logger.info("Successfully clicked View Online Manual button")
                            # Wait after clicking to ensure navigation starts
                            await asyncio.sleep(3)
                            return True
                        except Exception as e:
                            if attempt < max_retries - 1:
                                logger.warning(f"Click attempt {attempt + 1} failed, retrying... Error: {str(e)}")
                                await asyncio.sleep(2)
                            else:
                                logger.error(f"Failed to click button after {max_retries} attempts: {str(e)}")
                                return False
            
            logger.error("No visible View Online Manual button found")
            return False

        except Exception as e:
            logger.error(f"Error clicking View Online Manual button: {str(e)}")
            return False

    async def process_manual(self, page):
        """Process the currently open manual"""
        try:
            # Wait for page to load
            await page.wait_for_load_state('networkidle')
            await asyncio.sleep(3)  # Increased initial wait
            
            # Find the content frame with retries
            content_frame = None
            max_retries = 3
            retry_count = 0
            
            while retry_count < max_retries and not content_frame:
                frames = page.frames
                for frame in frames:
                    try:
                        if frame.url and 'mole.haynes.com' in frame.url:
                            content_frame = frame
                            break
                    except:
                        continue
                
                if not content_frame:
                    retry_count += 1
                    logger.info(f"Frame not found, retry {retry_count}/{max_retries}")
                    await asyncio.sleep(2)
            
            if not content_frame:
                logger.error("Could not find content frame after retries")
                return False

            # Extra wait after finding frame
            await asyncio.sleep(2)

            # Process the model with the content frame
            success = await self.process_model(content_frame)
            if success:
                logger.info(f"Successfully processed manual")
                return True
            else:
                logger.error(f"Failed to process manual")
                return False

        except Exception as e:
            logger.error(f"Error processing manual: {str(e)}")
            return False

    async def operation_with_timeout(self, operation, timeout_seconds, *args):
        """Run an operation with a timeout"""
        try:
            async def timeout_coro():
                await asyncio.sleep(timeout_seconds)
                raise TimeoutError(f"Operation timed out after {timeout_seconds} seconds")

            result = await asyncio.wait_for(operation(*args), timeout=timeout_seconds)
            return result
        except (TimeoutError, asyncio.TimeoutError) as e:
            logger.error(f"Operation timed out: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Operation failed: {str(e)}")
            return None

    def has_wiring_diagrams(self, make, model_folder):
        """Check if model has wiring diagrams section with valid PDFs"""
        # This method now just calls is_model_processed for consistency
        return self.is_model_processed(make, model_folder)

    def is_model_processed(self, make, folder_name):
        """Check if a model has been fully processed with all required sections"""
        try:
            # Skip models older than 1990
            years = [int(year) for year in folder_name.replace('-', ' ').split() if year.isdigit()]
            if years and any(year < 1990 for year in years):
                logger.info(f"Skipping pre-1990 model: {folder_name}")
                return True  # Return True to skip processing
                
            # Possible variations of "Wiring Diagrams" folder name
            wiring_diagram_folders = [
                "Wiring Diagrams",
                "Wiring Diagram",
                "wiring diagrams",
                "wiring diagram",
                "WIRING DIAGRAMS",
                "Electrical Wiring Diagrams",  # Add more variations as needed
                # ...
            ]

            model_path = None
            for folder in wiring_diagram_folders:
                potential_path = os.path.abspath(os.path.join(
                    self.manuals_dir,
                    make,
                    folder_name,
                    folder
                ))
                if os.path.exists(potential_path):
                    model_path = potential_path
                    break

            logger.info(f"DEBUG: Checking wiring diagrams path: {model_path}")

            if not model_path:
                logger.info(f"No wiring diagrams found for {folder_name} - needs processing")
                return False

            # Check for PDF files
            wiring_files = [f for f in os.listdir(model_path) if f.lower().endswith('.pdf')]
            logger.info(f"DEBUG: Found wiring diagram PDFs: {wiring_files}")

            if not wiring_files:
                logger.info(f"No wiring diagram PDFs found for {folder_name} - needs processing")
                return False

            # Verify PDFs are valid
            for pdf in wiring_files:
                pdf_path = os.path.abspath(os.path.join(model_path, pdf))
                size = os.path.getsize(pdf_path)
                logger.info(f"DEBUG: PDF {pdf} size: {size} bytes")
                if size < 1024:  # Less than 1KB
                    logger.info(f"Found invalid PDF {pdf} ({size} bytes) - needs processing")
                    return False

            logger.info(f"Model {folder_name} has valid wiring diagrams - skipping")
            return True

        except Exception as e:
            logger.error(f"Error checking model status: {str(e)}")
            return False

    def format_model_name(self, make, model_text):
        """Format model name consistently"""
        try:
            # Extract model name and year range
            match = re.search(rf"{make}\s+(.*?)\s*\((\d{{4}})\s*-\s*(\d{{4}})\)", model_text)
            if match:
                model_name, year1, year2 = match.groups()
                # Return just the model name and year range
                return f"{make} {model_name.strip()} {year1}-{year2}"
            return model_text  # Return as is if pattern doesn't match
        except Exception as e:
            logger.error(f"Error formatting model name: {str(e)}")
            return model_text

    def sanitize_filename(self, filename):
        """Sanitize filename by removing invalid characters and limiting length"""
        try:
            # Replace forward and back slashes with dashes
            filename = filename.replace('/', '-').replace('\\', '-')
            
            # Replace other invalid characters
            invalid_chars = ['<', '>', ':', '"', '|', '?', '*']
            for char in invalid_chars:
                filename = filename.replace(char, '')
            
            # Replace multiple spaces/dashes with single ones
            filename = ' '.join(filename.split())  # Normalize spaces
            filename = '-'.join(filter(None, filename.split('-')))  # Normalize dashes
            
            # Ensure filename doesn't end with a period or space
            filename = filename.strip('. ')
            
            # Limit length while preserving extension
            if filename.endswith('.pdf'):
                name_part = filename[:-4]
                if len(name_part) > 100:
                    name_part = name_part[:97] + '...'
                filename = name_part + '.pdf'
            else:
                if len(filename) > 100:
                    filename = filename[:97] + '...'
        
            return filename
        
        except Exception as e:
            logger.error(f"Error sanitizing filename: {str(e)}")
            # Return a safe default name if sanitization fails
            return f"document_{int(time.time())}.pdf"

    def create_save_path(self, main_menu, submenu):
        """Create the full absolute save path for the PDF"""
        try:
            if not self.current_vehicle:
                raise ValueError("No current vehicle information available")
        
            make, model_folder = self.current_vehicle
        
            # Clean up the menu names for folder/file names
            clean_main_menu = self.sanitize_filename(main_menu)
            clean_submenu = self.sanitize_filename(f"{submenu}.pdf")
        
            # Create the full path, ensuring each component is sanitized
            save_dir = os.path.join(
                self.manuals_dir,
                make,
                model_folder,
                clean_main_menu
            )
        
            # Ensure the directory exists
            os.makedirs(save_dir, exist_ok=True)
        
            # Create and return the full path
            full_path = os.path.join(save_dir, clean_submenu)
        
            # Verify the path length is within Windows limits (260 characters)
            if len(full_path) > 250:  # Leave some margin
                logger.warning(f"Path too long ({len(full_path)} chars), attempting to shorten")
                # Try to shorten the submenu filename while preserving the path
                max_submenu_len = 250 - len(os.path.dirname(full_path)) - 1
                if max_submenu_len > 10:  # Ensure we have enough space for a meaningful name
                    clean_submenu = self.sanitize_filename(submenu[:max_submenu_len-10] + '....pdf')
                    full_path = os.path.join(save_dir, clean_submenu)
                else:
                    # If path is still too long, use a timestamp-based name
                    clean_submenu = f"doc_{int(time.time())}.pdf"
                    full_path = os.path.join(save_dir, clean_submenu)
        
            return full_path
        
        except Exception as e:
            logger.error(f"Error creating save path: {str(e)}")
            return None

    async def try_save_pdf(self, page, save_path):
        """Try to save PDF with current settings"""
        try:
            # Try to click print button
            await page.click('button:has-text("Print")', timeout=2000)
            await asyncio.sleep(1)
            
            # Move to print button in system dialog
            abs_print_x, abs_print_y = self.get_absolute_coordinates(self.print_coords[0], self.print_coords[1])
            pyautogui.moveTo(abs_print_x, abs_print_y, duration=0.2)
            pyautogui.click()
            await asyncio.sleep(1)
            
            # Type save path
            pyautogui.write(save_path)
            await asyncio.sleep(1)
            
            # Click save button
            abs_save_x, abs_save_y = self.get_absolute_coordinates(self.save_coords[0], self.save_coords[1])
            pyautogui.moveTo(abs_save_x, abs_save_y, duration=0.2)
            pyautogui.click()
            await asyncio.sleep(1)
            
            # Verify save
            if await self.verify_pdf_save(save_path):
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Error in try_save_pdf: {str(e)}")
            return False

    async def run(self):
        """Main function to run the scraper"""
        async with async_playwright() as playwright:
            # Launch browser with specific window size
            browser = await playwright.chromium.launch(
                headless=False,
            )
            
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
            )
            
            page = await context.new_page()
            
            # Navigate to the base URL
            await page.goto(self.base_url)
            await page.wait_for_load_state('networkidle')
            
            # Get all makes first
            makes = await self.get_makes(page)
            if not makes:
                logger.error("No makes found")
                return
            
            # Process each make
            for make in makes:
                if make not in self.valid_makes:
                    logger.info(f"Skipping invalid make: {make}")
                    continue
                
                # Navigate to make's page
                make_url = self.get_make_url(make)
                while True:  # Keep processing the same make until all models are done
                    try:
                        await page.goto(make_url, wait_until='networkidle')
                    except Exception as e:
                        logger.error(f"Error navigating to {make}: {str(e)}")
                        await asyncio.sleep(2)  # Add delay before retrying
                        try:
                            await page.goto(self.base_url, wait_until='networkidle')  # Return to base URL
                            await asyncio.sleep(1)
                        except:
                            pass
                        break  # Move to next make if we can't access this one
                    
                    # Get models for this make - retry up to 3 times
                    models = None
                    for retry in range(3):
                        models = await self.get_models(page, make)
                        if models:
                            break
                        logger.info(f"Retry {retry + 1} getting models for {make}")
                        await asyncio.sleep(1)
                    
                    if not models:
                        logger.error(f"No models found for {make} after retries")
                        break  # Move to next make
                    
                    # Check if all models are processed
                    all_processed = True
                    for model in models:
                        folder_name = self.get_folder_name(model['name'])
                        if not self.is_model_processed(make, folder_name):
                            all_processed = False
                            break
                    
                    if all_processed:
                        logger.info(f"All models for {make} have been processed")
                        break  # Move to next make
                    
                    # Process each model
                    for model in models:
                        try:
                            # Convert model name to folder format
                            folder_name = self.get_folder_name(model['name'])
                            
                            # Set current vehicle info before processing
                            self.current_vehicle = (make, folder_name)
                            
                            # Check if model has been fully processed (has valid wiring diagrams)
                            if self.is_model_processed(make, folder_name):
                                logger.info(f"Skipping {make} {folder_name} - already has valid wiring diagrams")
                                continue
                            
                            # Step 1: Click the model link and wait for load
                            try:
                                await model['element'].click()
                                await page.wait_for_load_state('networkidle', timeout=10000)
                                await asyncio.sleep(2)
                            except Exception as e:
                                logger.error(f"Error clicking model link: {str(e)}")
                                await self.reset_to_make_page(page, make_url)
                                continue
                            
                            # Step 2: Extract year range and prepare to click version
                            year_match = re.search(r'\((\d{4})\s*-\s*(\d{4})\)', model['name'])
                            if not year_match:
                                logger.error(f"Could not extract year range from model: {model['name']}")
                                continue
                            
                            year1, year2 = year_match.groups()
                            year_range_with_space = f"{year1} - {year2}"
                            year_range_no_space = f"{year1}-{year2}"
                            
                            # Step 3: Click the version with appropriate year range
                            version_clicked = False
                            elements = await page.query_selector_all('*')
                            for element in elements:
                                try:
                                    text = await element.text_content()
                                    
                                    # Skip if no text
                                    if not text:
                                        continue
                                    
                                    # Clean up the text (remove extra whitespace)
                                    text = ' '.join(text.split())
                                    
                                    # Check if the text matches our pattern
                                    if text in [year_range_with_space, year_range_no_space]:
                                        # Verify this is a clickable link
                                        if await element.is_visible() and await element.is_enabled():
                                            await element.click()
                                            version_clicked = True
                                            await page.wait_for_load_state('networkidle')
                                            await asyncio.sleep(1)
                                            break
                                except:
                                    continue
                            
                            if not version_clicked:
                                logger.error(f"Could not click version for model: {folder_name}")
                                # Return to make's page for next model
                                await page.goto(make_url)
                                await page.wait_for_load_state('networkidle')
                                await asyncio.sleep(1)
                                continue
                            
                            # Step 4: Click View Online Manual button
                            if await self.click_view_manual_button(page):
                                await page.wait_for_load_state('networkidle')
                                await asyncio.sleep(2)
                            else:
                                logger.error(f"Could not click View Online Manual for model: {folder_name}")
                                # Return to make's page for next model
                                await page.goto(make_url)
                                await page.wait_for_load_state('networkidle')
                                await asyncio.sleep(1)
                                continue
                            
                            # Step 5: Process the manual
                            success = await self.process_manual(page)
                            if not success:
                                logger.error(f"Failed to process manual for {make} {folder_name}")
                                # Return to make's page for next model
                                await page.goto(make_url)
                                await page.wait_for_load_state('networkidle')
                                await asyncio.sleep(1)
                                continue
                            
                            logger.info(f"Successfully processed manual for {make} {folder_name}")
                            
                            # Return to make's page for next model
                            await page.goto(make_url)
                            await page.wait_for_load_state('networkidle')
                            await asyncio.sleep(1)
                            
                        except Exception as e:
                            logger.error(f"Error processing model {make} {folder_name}: {str(e)}")
                            # Try to return to make's page for next model
                            try:
                                await page.goto(make_url)
                                await page.wait_for_load_state('networkidle')
                                await asyncio.sleep(1)
                            except:
                                pass
                            continue
            
            await browser.close()

if __name__ == "__main__":
    try:
        scraper = BulkManualScraper()
        asyncio.run(scraper.run())
    except KeyboardInterrupt:
        logger.info("\nScript terminated by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
