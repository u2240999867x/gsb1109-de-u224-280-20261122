import sparse

import pytest

import numpy as np

einsum_cases = [
    "a,->a",
    "ab,->ab",
    ",ab,->ab",
    ",,->",
    "a,ab,abc->abc",
    "a,b,ab->ab",
    "ea,fb,gc,hd,abcd->efgh",
    "ea,fb,abcd,gc,hd->efgh",
    "abcd,ea,fb,gc,hd->efgh",
    "acdf,jbje,gihb,hfac,gfac,gifabc,hfac",
    "cd,bdhe,aidb,hgca,gc,hgibcd,hgac",
    "abhe,hidj,jgba,hiab,gab",
    "bde,cdh,agdb,hica,ibd,hgicd,hiac",
    "chd,bde,agbc,hiad,hgc,hgi,hiad",
    "chd,bde,agbc,hiad,bdi,cgh,agdb",
    "bdhe,acad,hiab,agac,hibd",
    "ab,ab,c->",
    "ab,ab,c->c",
    "ab,ab,cd,cd->",
    "ab,ab,cd,cd->ac",
    "ab,ab,cd,cd->cd",
    "ab,ab,cd,cd,ef,ef->",
    "ab,cd,ef->abcdef",
    "ab,cd,ef->acdf",
    "ab,cd,de->abcde",
    "ab,cd,de->be",
    "ab,bcd,cd->abcd",
    "ab,bcd,cd->abd",
    "eb,cb,fb->cef",
    "dd,fb,be,cdb->cef",
    "bca,cdb,dbf,afc->",
    "dcc,fce,ea,dbf->ab",
    "fdf,cdd,ccd,afe->ae",
    "abcd,ad",
    "ed,fcd,ff,bcf->be",
    "baa,dcf,af,cde->be",
    "bd,db,eac->ace",
    "fff,fae,bef,def->abd",
    "efc,dbc,acf,fd->abe",
    "ab,ab",
    "ab,ba",
    "abc,abc",
    "abc,bac",
    "abc,cba",
    "ab,bc",
    "ab,cb",
    "ba,bc",
    "ba,cb",
    "abcd,cd",
    "abcd,ab",
    "abcd,cdef",
    "abcd,cdef->feba",
    "abcd,efdc",
    "aab,bc->ac",
    "ab,bcc->ac",
    "aab,bcc->ac",
    "baa,bcc->ac",
    "aab,ccb->ac",
    "aab,fa,df,ecc->bde",
    "ecb,fef,bad,ed->ac",
    "bcf,bbb,fbf,fc->",
    "bb,ff,be->e",
    "bcb,bb,fc,fff->",
    "fbb,dfd,fc,fc->",
    "afd,ba,cc,dc->bf",
    "adb,bc,fa,cfc->d",
    "bbd,bda,fc,db->acf",
    "dba,ead,cad->bce",
    "aef,fbc,dca->bde",
    "abab->ba",
    "...ab,...ab",
    "...ab,...b->...a",
    "a...,a...",
    "a...,a...",
]


@pytest.mark.parametrize("subscripts", einsum_cases)
@pytest.mark.parametrize("density", [0.1, 1.0])
def test_einsum(subscripts, density):
    d = 4
    terms = subscripts.split("->")[0].split(",")
    arrays = [sparse.random((d,) * len(term), density=density) for term in terms]
    sparse_out = sparse.einsum(subscripts, *arrays)
    numpy_out = np.einsum(subscripts, *(s.todense() for s in arrays))

    if not numpy_out.shape:
        # scalar output — sparse_out is a 0-D COO per the Array API standard
        assert np.allclose(numpy_out, sparse_out.todense())
    else:
        # array output
        assert np.allclose(numpy_out, sparse_out.todense())


@pytest.mark.parametrize("input", [[[0, 0]], [[0, Ellipsis]], [[Ellipsis, 1], [Ellipsis]], [[0, 1], [0]]])
@pytest.mark.parametrize("density", [0.1, 1.0])
def test_einsum_nosubscript(input, density):
    d = 4
    arrays = [sparse.random((d, d), density=density)]
    sparse_out = sparse.einsum(*arrays, *input)
    numpy_out = np.einsum(*(s.todense() for s in arrays), *input)

    if not numpy_out.shape:
        # scalar output — sparse_out is a 0-D COO per the Array API standard
        assert np.allclose(numpy_out, sparse_out.todense())
    else:
        # array output
        assert np.allclose(numpy_out, sparse_out.todense())


def test_einsum_input_fill_value():
    x = sparse.random(shape=(2,), density=0.5, format="coo", fill_value=2)
    with pytest.raises(ValueError):
        sparse.einsum("cba", x)


def test_einsum_no_input():
    with pytest.raises(ValueError):
        sparse.einsum()


@pytest.mark.parametrize("subscript", ["a+b->c", "i->&", "i->ij", "ij->jij", "a..,a...", ".i...", "a,a->->"])
def test_einsum_invalid_input(subscript):
    x = sparse.random(shape=(2,), density=0.5, format="coo")
    y = sparse.random(shape=(2,), density=0.5, format="coo")
    with pytest.raises(ValueError):
        sparse.einsum(subscript, x, y)


@pytest.mark.parametrize("subscript", [0, [0, 0]])
def test_einsum_type_error(subscript):
    x = sparse.random(shape=(2,), density=0.5, format="coo")
    y = sparse.random(shape=(2,), density=0.5, format="coo")
    with pytest.raises(TypeError):
        sparse.einsum(subscript, x, y)


format_test_cases = [
    (("coo",), "coo"),
    (("dok",), "dok"),
    (("gcxs",), "gcxs"),
    (("dense",), "dense"),
    (("coo", "coo"), "coo"),
    (("dok", "coo"), "coo"),
    (("coo", "dok"), "coo"),
    (("coo", "dense"), "coo"),
    (("dense", "coo"), "coo"),
    (("dok", "dense"), "dok"),
    (("dense", "dok"), "dok"),
    (("gcxs", "dense"), "gcxs"),
    (("dense", "gcxs"), "gcxs"),
    (("dense", "dense"), "dense"),
    (("dense", "dok", "gcxs"), "coo"),
]


@pytest.mark.parametrize("formats,expected", format_test_cases)
def test_einsum_format(formats, expected, rng):
    inputs = [
        rng.standard_normal((2, 2, 2)) if format == "dense" else sparse.random((2, 2, 2), density=0.5, format=format)
        for format in formats
    ]
    if len(inputs) == 1:
        eq = "abc->bc"
    elif len(inputs) == 2:
        eq = "abc,cda->abd"
    elif len(inputs) == 3:
        eq = "abc,cad,dea->abe"

    out = sparse.einsum(eq, *inputs)
    assert {
        sparse.COO: "coo",
        sparse.DOK: "dok",
        sparse.GCXS: "gcxs",
        np.ndarray: "dense",
    }[out.__class__] == expected


def test_einsum_shape_check():
    x = sparse.random((2, 3, 4), density=0.5)
    with pytest.raises(ValueError):
        sparse.einsum("aab", x)
    y = sparse.random((2, 3, 4), density=0.5)
    with pytest.raises(ValueError):
        sparse.einsum("abc,acb", x, y)


@pytest.mark.parametrize("dtype", [np.int64, np.complex128])
def test_einsum_dtype(dtype):
    x = sparse.random((3, 3), density=0.5) * 10.0
    x = x.astype(np.float64)

    y = sparse.COO.from_numpy(np.ones((3, 1), dtype=np.float64))

    result = sparse.einsum("ij,i->j", x, y, dtype=dtype)

    assert result.dtype == dtype


def _index_selectors(n, entries, rng):
    """Build distinct sparse matrices with exactly ``entries`` stored values."""
    flat = rng.choice(n * n, size=entries, replace=False)
    coords = np.stack(np.unravel_index(flat, (n, n)))
    return coords, rng.random(entries)


def _budget_case_inputs(rng, n=60, entries=2 * 60):
    coords_a, data_a = _index_selectors(n, entries, rng)
    coords_b, data_b = _index_selectors(n, entries, rng)
    coords_c, data_c = _index_selectors(n, entries, rng)
    sites = sparse.COO(coords_a, data_a, shape=(n, n))
    rules = sparse.COO(coords_b, data_b, shape=(n, n))
    models = sparse.COO(coords_c, data_c, shape=(n, n))

    diag = np.arange(4)
    small_block = sparse.COO(np.stack([diag, diag]), np.ones(4), shape=(n, n))

    legal = np.sort(rng.choice(n, size=4, replace=False))
    few_combos = sparse.COO(np.stack([legal, legal]), np.ones(4), shape=(n, n))
    return sites, rules, models, small_block, few_combos


def _reference_chain(sites, rules, models, eligibility):
    dense = [array.todense() for array in (sites, rules, models)]
    eligibility = np.diag(np.diag(eligibility.todense()))
    return dense[0] @ dense[1] @ dense[2] @ eligibility @ np.ones_like(eligibility[:, 0])


@pytest.mark.parametrize("which", ["diagonal_block", "few_legal_combinations"])
def test_einsum_temp_budget_matches_reference(which, rng):
    sites, rules, models, small_block, few_combos = _budget_case_inputs(rng)
    eligibility = small_block if which == "diagonal_block" else few_combos
    assert sites.nnz == rules.nnz == models.nnz
    assert small_block.shape == few_combos.shape
    assert small_block.nnz == few_combos.nnz

    expected = _reference_chain(sites, rules, models, eligibility)
    result = sparse.einsum("ij,jk,kl,ll->i", sites, rules, models, eligibility, temp_elements=10_000)
    assert np.allclose(result.todense(), expected)


@pytest.mark.parametrize("which", ["diagonal_block", "few_legal_combinations"])
def test_einsum_temp_budget_order_invariance(which, rng):
    sites, rules, models, small_block, few_combos = _budget_case_inputs(rng)
    eligibility = small_block if which == "diagonal_block" else few_combos

    forward = sparse.einsum("ij,jk,kl,ll->i", sites, rules, models, eligibility, temp_elements=10_000)
    reversed_ = sparse.einsum("ll,kl,jk,ij->i", eligibility, models, rules, sites, temp_elements=10_000)
    expected = _reference_chain(sites, rules, models, eligibility)
    assert np.allclose(forward.todense(), expected)
    assert np.allclose(reversed_.todense(), expected)


def test_einsum_temp_budget_avoids_full_outer_product(rng, monkeypatch):
    sites, rules, models, small_block, _ = _budget_case_inputs(rng, n=40, entries=80)

    import sparse.numba_backend._common as common

    def fail_outer(*args, **kwargs):
        raise AssertionError("the ijkl outer product must not be materialised")

    # The original path multiplies all aligned operands at once via `mul`.
    monkeypatch.setattr(common, "mul", fail_outer)
    result = sparse.einsum("ij,jk,kl,ll->i", sites, rules, models, small_block, temp_elements=100)
    assert result.shape == (40,)


def test_einsum_temp_budget_reports_infeasible_contraction(rng):
    sites, rules, models, small_block, _ = _budget_case_inputs(rng, n=40, entries=80)
    with pytest.raises(sparse.EinsumBudgetError) as excinfo:
        sparse.einsum("ij,jk,kl,ll->i", sites, rules, models, small_block, temp_elements=1)

    message = str(excinfo.value)
    assert "ij,jk,kl,ll->i" in message
    assert "contraction" in message
    assert "over budget" in message
    assert isinstance(excinfo.value, MemoryError)


def test_einsum_temp_budget_sees_sparsity_difference(rng):
    from sparse.numba_backend._common import _pair_candidates, _prepare_einsum_factors

    sites, rules, models, small_block, few_combos = _budget_case_inputs(rng, n=30, entries=30)
    estimates = []
    for eligibility in (small_block, few_combos):
        factors, sizes = _prepare_einsum_factors(["ij", "jk", "kl", "ll"], "i", [sites, rules, models, eligibility])
        estimates.append([candidate[0] for candidate in _pair_candidates(factors, "i", sizes)])
    assert estimates[0] != estimates[1]


def test_einsum_temp_budget_validation():
    x = sparse.random((2, 2), density=0.5)
    y = sparse.random((2, 2), density=0.5)
    with pytest.raises(TypeError):
        sparse.einsum("ij,jk->ik", x, y, temp_elements=-1.5)
    with pytest.raises(ValueError):
        sparse.einsum("ij,jk->ik", x, y, temp_elements=-1)
    with pytest.raises(TypeError):
        sparse.einsum("ij,jk->ik", x, y, temp_elements=True)
    with pytest.raises(ValueError):
        sparse.einsum("ij,jk->ik", x, y, temp_elements=3, max_temp_elements=4)


@pytest.mark.parametrize("optimize", [True, "greedy", "auto", "optimal", None])
def test_einsum_optimize_matches_reference(optimize, rng):
    sites, rules, models, small_block, _ = _budget_case_inputs(rng)
    expected = _reference_chain(sites, rules, models, small_block)
    result = sparse.einsum("ij,jk,kl,ll->i", sites, rules, models, small_block, optimize=optimize)
    assert np.allclose(result.todense(), expected)


def test_einsum_optimize_validation():
    x = sparse.random((2, 2), density=0.5)
    y = sparse.random((2, 2), density=0.5)
    with pytest.raises(ValueError):
        sparse.einsum("ij,jk->ik", x, y, optimize="unknown")
    with pytest.raises(TypeError):
        sparse.einsum("ij,jk->ik", x, y, optimize=1.5)
