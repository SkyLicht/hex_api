import base64
import random
import string
import uuid

def generate_16_uuid() -> str:
    # Generate a UUID and remove the hyphens
    uuid_str = uuid.uuid4()

    # Convert UUID to bytes and then encode in Base64
    base64_uuid = base64.urlsafe_b64encode(uuid_str.bytes).decode("utf-8")

    # Remove specific unwanted characters
    clean_uuid = base64_uuid.translate(str.maketrans('', '', '/,-+*'))

    # Truncate the string to 16 characters
    short_uuid = clean_uuid[:16]

    return short_uuid


def generate_custom_id(length=15) -> str:
    # Define the characters for the custom ID
    characters = string.ascii_letters + string.digits

    # Randomly select `length` characters from the character set
    custom_id = ''.join(random.choices(characters, k=30))

    # Remove specific unwanted characters
    custom_id = custom_id.translate(str.maketrans('', '', '/,-+*'))

    return custom_id[:length]