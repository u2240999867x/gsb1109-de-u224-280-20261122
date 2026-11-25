import pytest

from tests.testmodels import StraightFields

pytestmark = pytest.mark.asyncio


async def test_full_refresh_makes_partial_instance_complete(db) -> None:
    instance = await StraightFields.create(chars="before")
    partial = await StraightFields.get(pk=instance.pk).only("eyedee", "chars")

    refreshed = await partial.refresh_from_db()

    assert refreshed is partial
    assert partial._partial is False

    partial.chars = "after"
    await partial.save()

    stored = await StraightFields.get(pk=instance.pk)
    assert stored.chars == "after"


async def test_partial_save_materializes_update_field_iterator(db) -> None:
    instance = await StraightFields.create(chars="before")
    partial = await StraightFields.get(pk=instance.pk).only("eyedee", "chars")
    partial.chars = "after iterator"

    await partial.save(update_fields=iter(("chars",)))

    stored = await StraightFields.get(pk=instance.pk)
    assert stored.chars == "after iterator"
