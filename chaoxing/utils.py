

def between(text: str, left: str, right: str) -> str | None:
    start_index = text.find(left)
    if start_index == -1:
        return None

    start_index += len(left)
    end_index = text.find(right, start_index)
    if end_index == -1:
        return None

    return text[start_index:end_index]