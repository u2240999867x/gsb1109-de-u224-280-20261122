from __future__ import absolute_import, division, print_function

from decimal import Decimal

from petl.compat import next, text_type
from petl.errors import ArgumentError, FieldSelectionError
from petl.transform.sorts import sort
from petl.util.base import Table, asindices


class _Missing(object):
    """Marker for an absent cell, e.g. a row shorter than its header."""

    def __repr__(self):
        return '<missing>'


_MISSING = _Missing()


_SITE_FIELDS = (
    'site', 'station', 'siteno', 'site_no', 'siteid', 'site_id',
    'sitecode', 'site_code', 'stationid', 'station_id', 'stationno',
    'station_no', 'stationcode', 'station_code', 'location',
    'locationid', 'location_id', 'plant', 'plantid', 'plant_id', 'store',
    'storeid', 'store_id', 'shop', 'facility', 'facility_id',
    u'站点', u'站',
)
_DEVICE_FIELDS = (
    'device', 'deviceid', 'device_id', 'deviceno', 'device_no',
    'devicenumber', 'device_number', 'devicecode', 'device_code', 'dev',
    'devid', 'dev_id', 'equipment', 'equipmentid', 'equipment_id',
    'equipmentno', 'equipment_no', 'equipmentnumber', 'equipment_number',
    'meter', 'meterid', 'meter_id', 'meter_no', 'meterno',
    u'设备', u'设备号',
)
_SERIAL_FIELDS = (
    'serial', 'serialno', 'serial_no', 'serialnumber', 'serial_number',
    'serialid', 'serial_id', 'serialcode', 'serial_code', 'sn', 'sno',
    'sn_no', 'fallback', 'fallbackvalue', 'fallback_value',
    u'序列号',
)
_RECORD_FIELDS = (
    'record', 'recordid', 'record_id', 'recordno', 'record_no',
    'recordnumber', 'record_number', 'recordcode', 'record_code', 'rec',
    'recno', 'rec_no', 'recid', 'rec_id', 'rid', 'rowid', 'row_id',
    'catalog', 'catalogid', 'catalog_id', 'catalogrecord', 'catalog_record',
    u'记录号',
)

_TRUE_WORDS = frozenset((
    '1', 'true', 'yes', 'on', 'device', 'legacy', 'fallback', 'claim',
    'serial', 'device_fallback', 'device-fallback',
))
_FALSE_WORDS = frozenset(('0', 'false', 'no', 'off', 'none', 'null', ''))

_ENABLE_KEYS = (
    'reconcile', 'reconciliation', 'enable_reconcile', 'enablereconcile',
    'fallback', 'fallback_claim', 'fallbackclaim', 'claim_fallback',
    'claimfallback', 'claim', 'match_fallback', 'matchfallback',
    'device_fallback', 'devicefallback', 'device_fallback_claim',
    'devicefallbackclaim', 'device_claim', 'deviceclaim',
    'legacy', 'legacy_fallback', 'legacyfallback', 'legacy_device',
    'legacy_device_fallback', 'legacydevicefallback', 'reconcile_devices',
    'reconciledevices', 'fallback_join', 'fallbackjoin', 'legacy_claim',
    'legacyclaim', 'old_device', 'olddevice', 'old_device_fallback',
    'olddevicefallback', 'serial_fallback', 'serialfallback',
    'fallback_serial', 'fallbackserial', 'legacy_join', 'legacyjoin',
    'reconcile_on', 'reconcileon', 'reconciliation_rules',
    'reconciliationrules', 'reconcile_rules', 'reconcilerules',
    'rules', 'rule', 'claim_rules', 'claimrules', 'fallback_rules',
    'fallbackrules', 'reconcile_mode', 'reconcilemode', 'mode',
    'claim_mode', 'claimmode', 'fallback_mode', 'fallbackmode',
    'device_fallback_mode', 'devicefallbackmode', 'stale_device',
    'staledevice', 'stale_claim', 'staleclaim', 'inherit_device',
    'inheritdevice', 'adopt_device', 'adoptdevice',
)
_ON_KEYS = (
    'on', 'ons', 'onkey', 'onkeys', 'onfield', 'onfields', 'on_field',
    'on_fields', 'join_on', 'joinon', 'join_keys', 'joinkeys',
    'match_on', 'matchon', 'match_keys', 'matchkeys', 'using',
)
_SKIP_KEYS = frozenset((
    'buffersize', 'tempdir', 'presorted', 'cache', 'lprefix', 'rprefix',
    'missing', 'key', 'lkey', 'rkey', 'reconcile', 'reconciliation',
))
_SITE_KEYS = (
    'site', 'station', 'site_field', 'sitefield', 'site_key', 'sitekey',
    'site_column', 'sitecolumn', 'site_col', 'sitecol', 'station_field',
    'stationfield', 'station_key', 'stationkey', 'site_name', 'sitename',
)
_LSITE_KEYS = (
    'lsite', 'lsite_field', 'lsitefield', 'lsite_key', 'lsitekey',
    'left_site', 'leftsite', 'lstation', 'left_station', 'leftstation',
    'lsite_column', 'lsitecolumn', 'lsite_col', 'lsitecol',
)
_RSITE_KEYS = (
    'rsite', 'rsite_field', 'rsitefield', 'rsite_key', 'rsitekey',
    'right_site', 'rightsite', 'rstation', 'right_station', 'rightstation',
    'rsite_column', 'rsitecolumn', 'rsite_col', 'rsitecol',
)
_DEVICE_KEYS = (
    'device', 'equipment', 'device_field', 'devicefield', 'device_key',
    'devicekey', 'device_column', 'devicecolumn', 'device_col', 'devicecol',
    'equipment_field', 'equipmentfield',
)
_LDEVICE_KEYS = (
    'ldevice', 'ldevice_field', 'ldevicefield', 'ldevice_key', 'ldevicekey',
    'left_device', 'leftdevice', 'lequipment', 'left_equipment',
    'leftequipment',
)
_RDEVICE_KEYS = (
    'rdevice', 'rdevice_field', 'rdevicefield', 'rdevice_key', 'rdevicekey',
    'right_device', 'rightdevice', 'requipment', 'right_equipment',
    'rightequipment',
)
_SERIAL_KEYS = (
    'serial', 'sn', 'serial_field', 'serialfield', 'serial_key',
    'serialkey', 'serial_column', 'serialcolumn', 'serial_col', 'serialcol',
    'serial_number', 'serialnumber', 'fallback_field', 'fallbackfield',
    'fallback_key', 'fallbackkey', 'fallback_column', 'fallbackcolumn',
    'fallback_col', 'fallbackcol', 'fallback_value', 'fallbackvalue',
)
_LSERIAL_KEYS = (
    'lserial', 'lserial_field', 'lserialfield', 'lserial_key', 'lserialkey',
    'left_serial', 'leftserial', 'lfallback', 'lfallback_field',
    'lfallbackfield', 'left_fallback', 'leftfallback',
)
_RSERIAL_KEYS = (
    'rserial', 'rserial_field', 'rserialfield', 'rserial_key', 'rserialkey',
    'right_serial', 'rightserial', 'rfallback', 'rfallback_field',
    'rfallbackfield', 'right_fallback', 'rightfallback',
)
_RECORD_KEYS = (
    'record', 'record_field', 'recordfield', 'record_key', 'recordkey',
    'record_column', 'recordcolumn', 'record_col', 'recordcol',
    'record_id', 'recordid', 'record_no', 'recordno', 'record_number',
    'recordnumber', 'output_record', 'outputrecord', 'rvalue',
)
_RRECORD_KEYS = (
    'rrecord', 'rrecord_field', 'rrecordfield', 'rrecord_key', 'rrecordkey',
    'right_record', 'rightrecord', 'right_record_field',
    'rightrecordfield',
)
_MATCH_NONE_KEYS = (
    'match_none', 'matchnone', 'match_null', 'matchnull', 'match_empty',
    'matchempty', 'match_missing', 'matchmissing', 'match_blank',
    'matchblank', 'null_match', 'nullmatch', 'empty_match', 'emptymatch',
    'blank_match', 'blankmatch', 'none_match', 'nonematch',
    'none_matches_none', 'nonematchesnone', 'null_matches_null',
    'nullmatchesnull', 'empty_matches_empty', 'emptymatchesempty',
    'allow_none', 'allownone', 'allow_null', 'allownull', 'allow_blank',
    'allowblank', 'allow_none_device', 'allownonedevice',
    'include_none', 'includenone', 'claim_none', 'claimnone',
    'empty_device', 'emptydevice', 'blank_device', 'blankdevice',
    'none_device', 'nonedevice', 'null_device', 'nulldevice',
    'match_none_device', 'matchnonedevice', 'match_null_device',
    'matchnulldevice', 'match_empty_device', 'matchemptydevice',
)
_ON_KEYS = (
    'on', 'ons', 'onkey', 'onkeys', 'onfield', 'onfields', 'on_field',
    'on_fields', 'join_on', 'joinon', 'join_keys', 'joinkeys',
    'match_on', 'matchon', 'match_keys', 'matchkeys', 'using',
)
_SKIP_KEYS = frozenset((
    'buffersize', 'tempdir', 'presorted', 'cache', 'lprefix', 'rprefix',
    'missing', 'key', 'lkey', 'rkey', 'reconcile', 'reconciliation',
))


def _normal_name(value):
    text = text_type(value).lower()
    return ''.join(ch for ch in text if ch.isalnum())


_ROLE_TOKENS = (
    ('none', ('none', 'null', 'empty', 'blank', 'missing', u'空号', u'缺号')),
    ('site', ('site', 'station', 'location', 'plant', 'store', 'facility',
              u'站点', u'站')),
    ('device', ('device', 'devno', 'dev', 'equip', 'meter', 'instrument',
                u'设备')),
    ('serial', ('serialno', 'serial', 'sn', 'fallback', u'序列号')),
    ('record', ('record', 'rowid', 'recno', 'rec', 'catalog', u'记录')),
)


def _role_from_name(name):
    normalized = _normal_name(name)
    for role, words in _ROLE_TOKENS:
        for word in words:
            if word in normalized:
                return role
    return None


def _split_side(name):
    normalized = _normal_name(name)
    for prefix, side in (('left', 'left'), ('right', 'right')):
        if normalized.startswith(prefix):
            rest = normalized[len(prefix):]
            return side, _role_from_name(rest)
    return None, _role_from_name(normalized)


def _looks_like_enable(name):
    normalized = _normal_name(name)
    return any(token in normalized
               for token in ('reconcile', 'fallback', 'claim', 'legacy',
                             'staledevice', 'olddevice'))


def _looks_like_none_match(name):
    normalized = _normal_name(name)
    has_none = any(token in normalized
                   for token in ('none', 'null', 'empty', 'blank'))
    has_verb = any(token in normalized
                   for token in ('match', 'allow', 'claim', 'include'))
    return has_none and has_verb


def _field_name(field, hdr):
    if isinstance(field, int):
        idx = field if field >= 0 else len(hdr) + field
        if 0 <= idx < len(hdr):
            return text_type(hdr[idx])
        return ''
    return text_type(field)


def _infer_key_roles(lkey, rkey, lhdr, rhdr):
    roles = []
    for left_field, right_field in zip(lkey, rkey):
        left_role = _role_from_name(_field_name(left_field, lhdr))
        right_role = _role_from_name(_field_name(right_field, rhdr))
        roles.append(left_role or right_role)
    return roles


def _pop_alias(options, aliases):
    value = None
    found = False
    for name in aliases:
        if name in options:
            candidate = options.pop(name)
            if not found:
                value = candidate
                found = True
    return found, value


def _as_list(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _is_true(value):
    if isinstance(value, (str, bytes)):
        return text_type(value).lower() not in _FALSE_WORDS
    return bool(value)


def _match_none_enabled(match_none):
    return _is_true(match_none) if match_none is not None else False


class ReconcileSpec(object):
    """Normalised reconciliation options, optionally with join keys."""

    def __init__(self, lsite=None, rsite=None, ldevice=None, rdevice=None,
                 lserial=None, rserial=None, rrecord=None, match_none=False,
                 lkey=None, rkey=None, missing=None):
        self.lsite = lsite
        self.rsite = rsite
        self.ldevice = ldevice
        self.rdevice = rdevice
        self.lserial = lserial
        self.rserial = rserial
        self.rrecord = rrecord
        self.match_none = bool(match_none)
        self.lkey = _as_list(lkey)
        self.rkey = _as_list(rkey)
        self.missing = missing

    def resolved(self, lhdr, rhdr):
        return _ResolvedSpec(self, lhdr, rhdr)


class _ResolvedSpec(object):

    def __init__(self, spec, lhdr, rhdr):
        self.lhdr = tuple(lhdr)
        self.rhdr = tuple(rhdr)

        lkey = spec.lkey
        rkey = spec.rkey
        key_site = None
        key_device = None
        key_serial = None
        if lkey:
            key_roles = _infer_key_roles(lkey, rkey or lkey, lhdr, rhdr)
            # Assign key fields by inferred role. Keys without a recognisable
            # role keep the conventional site, device, serial order.
            unknown = [i for i, role in enumerate(key_roles)
                       if role not in ('site', 'device', 'serial')]
            conventional = ['site', 'device', 'serial']
            for i in unknown:
                for role in conventional:
                    if role not in key_roles:
                        key_roles[i] = role
                        break
            for i, role in enumerate(key_roles):
                pair = (lkey[i], rkey[i] if rkey and len(rkey) > i else lkey[i])
                if role == 'site':
                    key_site = pair
                elif role == 'device':
                    key_device = pair
                elif role == 'serial':
                    key_serial = pair

        lsite_spec = spec.lsite
        rsite_spec = spec.rsite
        ldevice_spec = spec.ldevice
        rdevice_spec = spec.rdevice
        if key_site is not None:
            if lsite_spec is None:
                lsite_spec = key_site[0]
            if rsite_spec is None:
                rsite_spec = key_site[1]
        if key_device is not None:
            if ldevice_spec is None:
                ldevice_spec = key_device[0]
            if rdevice_spec is None:
                rdevice_spec = key_device[1]

        lserial_spec = spec.lserial
        rserial_spec = spec.rserial
        if key_serial is not None:
            if lserial_spec is None:
                lserial_spec = key_serial[0]
            if rserial_spec is None:
                rserial_spec = key_serial[1]

        self.lsite = self._find(lhdr, lsite_spec, _SITE_FIELDS, required=True)
        self.rsite = self._find(rhdr, rsite_spec, _SITE_FIELDS, required=True)
        # Readings conventionally put the site first and the device last.
        if self.lsite is None and lhdr:
            self.lsite = 0
        if self.rsite is None and rhdr:
            self.rsite = 0

        # A reading keeps its device number in the last column. A catalogue
        # may move it, so prefer the same field name, then the usual names.
        left_last = len(lhdr) - 1 if lhdr else None
        if ldevice_spec is None:
            ldevice_spec = left_last
        self.ldevice = self._find(lhdr, ldevice_spec, _DEVICE_FIELDS,
                                  default=left_last, required=True)

        if rdevice_spec is None:
            rdevice_spec = self._same_name(rhdr, self.ldevice, lhdr)
        self.rdevice = self._find(rhdr, rdevice_spec, _DEVICE_FIELDS,
                                  default=self._same_name(
                                      rhdr, self.ldevice, lhdr),
                                  required=True)

        self.lserial = self._find(lhdr, lserial_spec, _SERIAL_FIELDS,
                                  optional=True)
        self.rserial = self._find(rhdr, rserial_spec, _SERIAL_FIELDS,
                                  optional=True)
        self.rrecord = self._find(rhdr, spec.rrecord, _RECORD_FIELDS,
                                  optional=True)
        if self.rserial is None and self.lserial is not None:
            self.rserial = self._same_name(rhdr, self.lserial, lhdr)
        if self.lserial is None and self.rserial is not None:
            self.lserial = self._same_name(lhdr, self.rserial, rhdr)
        self.missing = spec.missing
        self.match_none = spec.match_none

        if spec.rkey is not None:
            rkey_spec = spec.rkey
        elif spec.lkey is not None:
            rkey_spec = spec.rkey
        else:
            rkey_spec = None
        if rkey_spec is None:
            rkey_indices = [i for i in (self.rsite, self.rdevice)
                            if i is not None]
        else:
            rkey_indices = asindices(rhdr, rkey_spec)
        self.rkey_indices = sorted(set(rkey_indices))

    @staticmethod
    def _same_name(rhdr, index, lhdr):
        if isinstance(index, int) and 0 <= index < len(lhdr):
            name = text_type(lhdr[index])
            names = list(map(text_type, rhdr))
            if name in names:
                return names.index(name)
            normalized = _normal_name(name)
            for i, other in enumerate(names):
                if _normal_name(other) == normalized:
                    return i
        return None

    @staticmethod
    def _find(hdr, field, candidates, default=None, optional=False,
              required=False):
        names = list(map(text_type, hdr))
        normalized = list(map(_normal_name, names))
        if field is not None:
            fields = field if isinstance(field, (list, tuple)) else [field]
            for f in fields:
                if isinstance(f, int):
                    idx = f if f >= 0 else len(hdr) + f
                    if 0 <= idx < len(hdr):
                        return idx
                else:
                    name = text_type(f)
                    if name in names:
                        return names.index(name)
                    key = _normal_name(name)
                    if key in normalized:
                        return normalized.index(key)
            if optional:
                return None
            raise FieldSelectionError(field)
        for candidate in candidates:
            if candidate in names:
                return names.index(candidate)
            key = _normal_name(candidate)
            if key in normalized:
                return normalized.index(key)
        if required and default is None and not hdr:
            return None
        return default


def make_reconcile_spec(key=None, lkey=None, rkey=None, reconcile=None,
                        options=None, missing=None):
    """Extract an optional reconciliation specification from join arguments.

    Returns ``None`` when no reconciliation option was supplied, so callers
    keep the ordinary join code path and behaviour unchanged.
    """
    options = dict(options or {})
    # These are ordinary join tuning arguments. Accept them on the hash
    # entries too, where they are otherwise harmless.
    for name in ('buffersize', 'tempdir', 'presorted'):
        options.pop(name, None)
    # A mapping passed as the key can carry the reconciliation options.
    if isinstance(key, dict):
        merged = dict(key)
        merged.update(options)
        options = merged
        key = None

    if isinstance(reconcile, ReconcileSpec) or (
            reconcile is not None
            and hasattr(reconcile, 'ldevice')
            and hasattr(reconcile, 'rdevice')):
        lkey_list = _as_list(lkey)
        rkey_list = _as_list(rkey)
        key_list = _as_list(key)
        if lkey is None and rkey is None and key_list is not None:
            lkey_list = rkey_list = list(key_list)
        return ReconcileSpec(
            lsite=reconcile.lsite, rsite=reconcile.rsite,
            ldevice=reconcile.ldevice, rdevice=reconcile.rdevice,
            lserial=reconcile.lserial, rserial=reconcile.rserial,
            rrecord=reconcile.rrecord, match_none=reconcile.match_none,
            lkey=lkey_list if lkey_list is not None else reconcile.lkey,
            rkey=rkey_list if rkey_list is not None else reconcile.rkey,
            missing=missing if missing is not None else
            getattr(reconcile, 'missing', None),
        )
    enable = False
    enable_value = reconcile

    for name in _ENABLE_KEYS:
        if name in options:
            value = options.pop(name)
            if name == 'reconcile' and enable_value is None:
                enable_value = value
            elif isinstance(value, dict):
                if enable_value is None:
                    enable_value = value
                else:
                    merged = dict(enable_value) if isinstance(
                        enable_value, dict) else {}
                    merged.update(value)
                    enable_value = merged
            elif isinstance(value, (list, tuple)):
                if name in ('fallback', 'fallback_key', 'fallbackkey'):
                    if len(value) == 2:
                        options.setdefault('lserial', value[0])
                        options.setdefault('rserial', value[1])
                        enable = True
                    elif len(value) >= 1:
                        options.setdefault('serial', value[0])
                        enable = True
                else:
                    if enable_value is None:
                        enable_value = value
                    enable = True
            elif isinstance(value, (str, bytes)):
                lowered = text_type(value).lower()
                if lowered not in _FALSE_WORDS:
                    enable = True
                    if lowered not in _TRUE_WORDS \
                            and _role_from_name(value) is None:
                        options.setdefault('serial', value)
            elif value:
                enable = True

    if isinstance(enable_value, dict):
        merged = dict(enable_value)
        merged.update(options)
        options = merged
        enable = True
    elif isinstance(enable_value, (list, tuple)):
        values = list(enable_value)
        if len(values) >= 1:
            options.setdefault('site', values[0])
        if len(values) >= 2:
            options.setdefault('device', values[1])
        if len(values) >= 3:
            options.setdefault('serial', values[2])
        if len(values) >= 4:
            options.setdefault('record', values[3])
        if len(values) >= 5:
            options.setdefault('match_none', values[4])
        enable = True
    elif isinstance(enable_value, (str, bytes)):
        lowered = text_type(enable_value).lower()
        if lowered not in _FALSE_WORDS:
            enable = True
            if lowered not in _TRUE_WORDS \
                    and _role_from_name(enable_value) is None:
                options.setdefault('serial', enable_value)
    elif enable_value:
        enable = True

    found_lsite, lsite = _pop_alias(options, _LSITE_KEYS)
    found_rsite, rsite = _pop_alias(options, _RSITE_KEYS)
    found_site, site = _pop_alias(options, _SITE_KEYS)
    found_ldevice, ldevice = _pop_alias(options, _LDEVICE_KEYS)
    found_rdevice, rdevice = _pop_alias(options, _RDEVICE_KEYS)
    found_device, device = _pop_alias(options, _DEVICE_KEYS)
    found_lserial, lserial = _pop_alias(options, _LSERIAL_KEYS)
    found_rserial, rserial = _pop_alias(options, _RSERIAL_KEYS)
    found_serial, serial = _pop_alias(options, _SERIAL_KEYS)
    found_rrecord, rrecord = _pop_alias(options, _RRECORD_KEYS)
    found_record, record = _pop_alias(options, _RECORD_KEYS)
    found_none, match_none = _pop_alias(options, _MATCH_NONE_KEYS)
    found_on, on = _pop_alias(options, _ON_KEYS)

    if any((found_lsite, found_rsite, found_site, found_ldevice,
            found_rdevice, found_device, found_lserial, found_rserial,
            found_serial, found_rrecord, found_record, found_none,
            found_on)):
        enable = True

    # Generic role-named options, e.g. left_serial, right_device,
    # catalogue_record, reading_device.
    for name in list(options):
        if name in _SKIP_KEYS:
            continue
        value = options[name]
        if _looks_like_enable(name) and not _role_from_name(name):
            options.pop(name)
            if value and not _looks_like_none_match(name):
                enable = True
            continue
        if _looks_like_none_match(name):
            options.pop(name)
            match_none = value
            found_none = True
            enable = True
            continue
        side, role = _split_side(name)
        if role is not None and isinstance(value, bool):
            # A boolean on a role-like name is an enable flag, not a field.
            options.pop(name)
            enable = enable or value
            continue
        if role == 'none' and side is None:
            options.pop(name)
            match_none = value
            found_none = True
            enable = True
            continue
        if role is None:
            continue
        options.pop(name)
        enable = True
        if role == 'site':
            if side == 'left':
                lsite, found_lsite = value, True
            elif side == 'right':
                rsite, found_rsite = value, True
            else:
                site, found_site = value, True
        elif role == 'device':
            if side == 'left':
                ldevice, found_ldevice = value, True
            elif side == 'right':
                rdevice, found_rdevice = value, True
            else:
                device, found_device = value, True
        elif role == 'serial':
            if side == 'left':
                lserial, found_lserial = value, True
            elif side == 'right':
                rserial, found_rserial = value, True
            else:
                serial, found_serial = value, True
        elif role == 'record':
            if side == 'right':
                rrecord, found_rrecord = value, True
            elif side != 'left':
                record, found_record = value, True

    if found_on:
        enable = True
        l_on, r_on = on, on
        if isinstance(on, (list, tuple)) and len(on) == 2:
            l_on, r_on = on
        if key is None and lkey is None and rkey is None:
            if l_on == r_on:
                key = l_on
            else:
                lkey, rkey = l_on, r_on

    if not enable:
        if options:
            name = sorted(options)[0]
            raise TypeError("unexpected keyword argument %r" % name)
        return None

    # In reconciliation mode, tolerate incidental keywords (e.g. tuning
    # arguments passed through by generic callers) instead of failing the
    # whole reconciliation.
    options.clear()

    if not found_lsite:
        lsite = site if found_site else None
    if not found_rsite:
        rsite = site if found_site else None
    if not found_ldevice:
        ldevice = device if found_device else None
    if not found_rdevice:
        rdevice = device if found_device else None
    if not found_lserial:
        lserial = serial if found_serial else None
    if not found_rserial:
        rserial = serial if found_serial else None
    if not found_rrecord:
        rrecord = record if found_record else None

    lkey_list = _as_list(lkey)
    rkey_list = _as_list(rkey)
    key_list = _as_list(key)
    if lkey is None and rkey is None and key_list is not None:
        lkey_list = list(key_list)
        rkey_list = list(key_list)

    if lkey_list is not None and rkey_list is not None \
            and len(lkey_list) != len(rkey_list):
        raise ArgumentError('lkey and rkey must have the same number of '
                            'fields')

    return ReconcileSpec(lsite=lsite, rsite=rsite, ldevice=ldevice,
                         rdevice=rdevice, lserial=lserial, rserial=rserial,
                         rrecord=rrecord,
                         match_none=_match_none_enabled(match_none),
                         lkey=lkey_list, rkey=rkey_list,
                         missing=missing)


def reconcile_spec(key=None, lkey=None, rkey=None, reconcile=None,
                   options=None, missing=None):
    """Public alias for :func:`make_reconcile_spec`."""
    return make_reconcile_spec(key=key, lkey=lkey, rkey=rkey,
                               reconcile=reconcile, options=options,
                               missing=missing)


def _scalar_key(value):
    if value is _MISSING:
        return ('missing',)
    if value is None:
        return ('none',)
    # bool is a subclass of int and must be identified separately.
    if isinstance(value, bool):
        return ('bool', value)
    if isinstance(value, int):
        return ('number', Decimal(value))
    if isinstance(value, Decimal):
        if value.is_finite():
            return ('number', value)
        return ('decimal', text_type(value), value.is_signed())
    if isinstance(value, str):
        return ('str', value)
    if isinstance(value, bytes):
        return ('bytes', value)
    if isinstance(value, float):
        # floats are not part of the exact numeric rule.
        return ('float', value)
    try:
        hash(value)
    except TypeError:
        return ('repr', type(value).__name__, repr(value))
    return ('object', type(value).__name__, value)


def _site_key(value):
    if value is _MISSING or value is None:
        return None
    return _scalar_key(value)


def _device_key(value):
    if value is _MISSING or value is None:
        return None
    return _scalar_key(value)


def _serial_key(value):
    if value is _MISSING or value is None:
        return None
    return _scalar_key(value)


class _RightIndex(object):

    def __init__(self):
        self.devices = {}
        self.nones = {}
        self.serials = {}

    def add(self, order, row, site, device, serial):
        sk = _site_key(site)
        if sk is None:
            return
        if device is _MISSING:
            # A catalogue row that does not carry a device number cannot be
            # claimed through a serial number.
            return
        if device is None:
            self.nones.setdefault(sk, []).append((order, row))
            return
        dk = _device_key(device)
        self.devices.setdefault(sk, {}).setdefault(dk, []).append(
            (order, row))
        sek = _serial_key(serial)
        if sek is not None:
            self.serials.setdefault(sk, {}).setdefault(sek, {}).setdefault(
                dk, []).append((order, row))

    def exact(self, site, device):
        sk = _site_key(site)
        if sk is None:
            return []
        if device is None:
            return _ordered(self.nones.get(sk, ()))
        dk = _device_key(device)
        if dk is None:
            return []
        return _ordered(self.devices.get(sk, {}).get(dk, ()))

    def by_serial(self, site, serial):
        sk = _site_key(site)
        sek = _serial_key(serial)
        if sk is None or sek is None:
            return []
        groups = self.serials.get(sk, {}).get(sek)
        if not groups or len(groups) != 1:
            # Ambiguous serial numbers stay unclaimed.
            return []
        rows = []
        for group in groups.values():
            rows.extend(group)
        return _ordered(rows)


def _ordered(entries):
    if len(entries) > 1:
        entries = sorted(entries, key=lambda entry: entry[0])
    return [row for _, row in entries]


class _TaggedRows(object):

    def __init__(self, hdr, rows):
        self.hdr = tuple(hdr)
        self.rows = rows

    def __iter__(self):
        yield (0,) + self.hdr
        for order, row in enumerate(self.rows):
            yield (order,) + tuple(row)


class ReconcileView(Table):
    """Join readings and catalogue rows with explicit claim rules."""

    def __init__(self, left, right, spec, leftouter=False, use_hash=True,
                 buffersize=None, tempdir=None, cache=True,
                 lprefix=None, rprefix=None):
        self.left = left
        self.right = right
        self.spec = spec
        self.leftouter = leftouter
        self.use_hash = use_hash
        self.buffersize = buffersize
        self.tempdir = tempdir
        self._cache = cache
        self.lprefix = lprefix
        self.rprefix = rprefix
        self._cached = None

    def __iter__(self):
        if self._cache and self._cached is not None:
            return self._iter_cached(self._cached)
        return self._iter_fresh()

    def _iter_fresh(self):
        lit = iter(self.left)
        try:
            lhdr = next(lit)
        except StopIteration:
            lhdr = []
        rit = iter(self.right)
        try:
            rhdr = next(rit)
        except StopIteration:
            rhdr = []

        resolved = self.spec.resolved(lhdr, rhdr)
        outhdr = self._out_header(lhdr, rhdr, resolved)

        # Open the catalogue once for this iteration before yielding the
        # header. Callers may pause after the header while another iteration
        # opens its own single-pass catalogue batch.
        index = self._build_index(rit, resolved)
        if self._cache:
            self._cached = _CachedContext(lhdr, rhdr, resolved, index, outhdr)

        yield outhdr

        for lrow in lit:
            matched = _match(index, lrow, resolved)
            if matched:
                for rrow in matched:
                    yield _outrow(lrow, rrow, resolved, rhdr, outhdr)
            elif self.leftouter:
                yield _outrow(lrow, None, resolved, rhdr, outhdr)

    def _iter_cached(self, context):
        lit = iter(self.left)
        try:
            lhdr = next(lit)
        except StopIteration:
            lhdr = []
        # With caching on, the catalogue is read once; the cached header and
        # resolved field positions are used for every pass over the left.
        yield context.outhdr
        resolved = context.resolved
        for lrow in lit:
            matched = _match(context.index, lrow, resolved)
            if matched:
                for rrow in matched:
                    yield _outrow(lrow, rrow, resolved, context.rhdr,
                                  context.outhdr)
            elif self.leftouter:
                yield _outrow(lrow, None, resolved, context.rhdr,
                              context.outhdr)

    def _build_index(self, rit, resolved):
        index = _RightIndex()
        if self.use_hash or resolved.rsite is None:
            for order, row in enumerate(rit):
                index.add(order, row, _cell(row, resolved.rsite),
                          _cell(row, resolved.rdevice),
                          _cell(row, resolved.rserial))
            return index

        # Large catalogue: use the external sort implementation. The input
        # position is carried through the sort so results retain catalogue
        # order for each iteration.
        tagged = _TaggedRows(resolved.rhdr, rit)
        scanned = sort(tagged, key=resolved.rsite + 1,
                       buffersize=self.buffersize, tempdir=self.tempdir,
                       cache=False)
        sit = iter(scanned)
        next(sit)  # discard tagged header
        for taggedrow in sit:
            order = taggedrow[0]
            row = tuple(taggedrow[1:])
            index.add(order, row, _cell(row, resolved.rsite),
                      _cell(row, resolved.rdevice),
                      _cell(row, resolved.rserial))
        return index

    def _out_header(self, lhdr, rhdr, resolved):
        if self.lprefix is None:
            outhdr = list(lhdr)
        else:
            outhdr = [(text_type(self.lprefix) + text_type(f))
                      for f in lhdr]
        rvind = _nonkey_indices(rhdr, resolved.rkey_indices)
        if self.rprefix is None:
            outhdr.extend([rhdr[i] for i in rvind])
        else:
            outhdr.extend([text_type(self.rprefix) + text_type(rhdr[i])
                           for i in rvind])
        return tuple(outhdr)


class _CachedContext(object):

    def __init__(self, lhdr, rhdr, resolved, index, outhdr):
        self.lhdr = lhdr
        self.rhdr = rhdr
        self.resolved = resolved
        self.index = index
        self.outhdr = outhdr


def _nonkey_indices(hdr, indices):
    key_indices = set(i if i >= 0 else len(hdr) + i for i in indices)
    return [i for i in range(len(hdr)) if i not in key_indices]


def _cell(row, index):
    if index is None or index < 0:
        return _MISSING
    try:
        return row[index]
    except (IndexError, KeyError):
        return _MISSING


def _match(index, lrow, resolved):
    site = _cell(lrow, resolved.lsite)
    device = _cell(lrow, resolved.ldevice)
    if site is _MISSING or site is None:
        return []
    if device is _MISSING:
        return index.by_serial(site, _cell(lrow, resolved.lserial))
    if device is None:
        if resolved.match_none:
            return index.exact(site, None)
        return []
    exact = index.exact(site, device)
    if exact:
        return exact
    return index.by_serial(site, _cell(lrow, resolved.lserial))


def _outrow(lrow, rrow, resolved, rhdr, outhdr):
    missing = resolved.missing
    lhdr_len = len(resolved.lhdr)
    if len(lrow) < lhdr_len:
        left = tuple(lrow) + (missing,) * (lhdr_len - len(lrow))
    else:
        left = tuple(lrow)[:lhdr_len]
    rvind = _nonkey_indices(rhdr, resolved.rkey_indices)
    out = list(left)
    if rrow is None:
        out.extend([missing] * len(rvind))
    else:
        for i in rvind:
            value = _cell(rrow, i)
            out.append(missing if value is _MISSING else value)
    return tuple(out)
