import hashlib

while True:
    user_input = input("Enter a string: ")
    hash_object = hashlib.sha1(user_input.encode())
    print("SHA-1 hash:", hash_object.hexdigest())
