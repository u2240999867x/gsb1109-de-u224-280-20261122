'''Identifies a Tap that contributes streams to a confluence.

Two independent Taps can publish streams with the same local name (for
example, both calling their detail stream ``items``). A :class:`SourceId`
keeps those contributions distinct inside a merged stream so that a shared
public key can never be mistaken for a redelivery from the same source.
'''


class SourceId:
    '''An immutable identifier for a Tap participating in a confluence.

    ``name`` is the stable, machine-readable identifier of the Tap. It is
    used to namespace bookmarks and contracts, so it must not change between
    runs. ``display_name`` is optional and is used in log lines and error
    messages that operators read.
    '''

    def __init__(self, name, display_name=None):
        if not isinstance(name, str) or not name:
            raise ValueError("SourceId name must be a non-empty string")
        if '.' in name:
            raise ValueError(
                "SourceId name must not contain '.': {}".format(name))
        self.name = name
        self.display_name = display_name or name

    def __eq__(self, other):
        return isinstance(other, SourceId) and other.name == self.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return self.name

    def __repr__(self):
        return 'SourceId(name={!r}, display_name={!r})'.format(
            self.name, self.display_name)

    def to_dict(self):
        result = {'name': self.name}
        if self.display_name != self.name:
            result['display_name'] = self.display_name
        return result

    @classmethod
    def from_dict(cls, data):
        if isinstance(data, SourceId):
            return data
        if isinstance(data, str):
            return cls(data)
        return cls(data['name'], data.get('display_name'))
