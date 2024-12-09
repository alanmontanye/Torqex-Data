import os
import re

def shorten_filename(filename):
    # Remove common words and abbreviate
    replacements = {
        'check and': '',
        'check': 'chk',
        'replacement': 'repl',
        'maintenance': 'maint',
        'adjustment': 'adj',
        'system': 'sys',
        'models only': '',
        'Integra': 'Int',
        'Legend': 'Lgd',
        'automatic': 'auto',
        'manual': 'man',
        'transaxle': 'trans',
    }
   
    new_name = filename.lower()
    for old, new in replacements.items():
        new_name = new_name.replace(old.lower(), new)
    
    # Keep the number prefix if it exists
    number_match = re.match(r'(\d+)\s*', new_name)
    prefix = number_match.group(1) if number_match else ''
    
    # Remove extra spaces and make title case
    new_name = ' '.join(new_name.split())
    new_name = new_name.title()
    
    # Add .pdf if it was removed
    if not new_name.endswith('.pdf'):
        new_name += '.pdf'
    
    return new_name

def rename_files_in_directory(directory):
    for root, dirs, files in os.walk(directory):
        for filename in files:
            if filename.endswith('.pdf'):
                old_path = os.path.join(root, filename)
                new_filename = shorten_filename(filename)
                new_path = os.path.join(root, new_filename)
                
                # Only rename if the new name is different
                if old_path != new_path:
                    try:
                        os.rename(old_path, new_path)
                        print(f'Renamed:\n  From: {filename}\n  To:   {new_filename}\n')
                    except Exception as e:
                        print(f'Error renaming {filename}: {str(e)}')

if __name__ == '__main__':
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'downloaded_manuals')
    acura_dir = os.path.join(base_dir, 'Acura', 'Acura Integra 1986-1989')
    
    print(f'Starting to rename files in: {acura_dir}\n')
    rename_files_in_directory(acura_dir)
    print('Finished renaming files.')
