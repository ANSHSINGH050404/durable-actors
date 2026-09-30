from pydantic import BaseModel, Field

from durable_actors import Actor, emitted, persisted


class Note(BaseModel):
    """A saved note with its original text."""

    text: str = Field(description="The note text, including whitespace.")


class Notebook(Actor):
    r'''Store notes in a shared notebook.

    Examples may contain """quotes""", backslashes like \notes, and Unicode: café.
    '''

    notes: list[Note] = emitted(persisted(default_factory=list))

    def save(self, note: Note) -> Note:
        """Save a note and return the saved value.

        Args:
            note: The note to append.

        Returns:
            The saved note, including its original whitespace.
        """
        self.notes.append(note)
        return note

    def clear(self) -> None:
        self.notes.clear()
