import numpy as np
import pandas as pd
from ncls import NCLS  # type: ignore


def add_rows_per_group(df):
    last_rows = df.groupby("__ix__", observed=False).last().reset_index()
    last_rows["__last__"] = True
    df = pd.concat([df, last_rows], ignore_index=True)
    df = df.sort_values("__ix__", ascending=True)
    return df


# def _subtraction(scdf, **kwargs):
#     if scdf.empty:
#         return scdf

#     falses = np.zeros(len(scdf), dtype=bool)
#     scdf.insert(scdf.shape[1], "__first__", falses)
#     scdf.insert(scdf.shape[1], "__last__", falses)

#     scdf = add_rows_per_group(scdf)

#     scdf.insert(scdf.shape[1], "NewStart", scdf.End__deleteme__.shift(fill_value=-1))
#     scdf.insert(scdf.shape[1], "NewEnd", scdf.Start__deleteme__)
#     scdf.insert(scdf.shape[1], "__ix2__", np.arange(len(scdf)))

#     first_rows = scdf.groupby(scdf.__ix__, as_index=False).first()
#     scdf.loc[scdf.__ix2__.isin(first_rows.__ix2__), "__first__"] = True

#     scdf.loc[:, "NewStart"] = np.where(scdf.__first__, scdf.Start, scdf.NewStart)

#     scdf.loc[scdf.__first__ & ~(scdf.Start__deleteme__ >= scdf.Start), "NewStart"] = -1
#     scdf.loc[:, "NewEnd"] = np.where(scdf.__last__, scdf.End, scdf.NewEnd)
#     scdf.loc[:, "NewStart"] = np.where(scdf.__last__, scdf.End__deleteme__, scdf.NewStart)
#     scdf.loc[scdf.__last__ & ~(scdf.End__deleteme__ <= scdf.End), ["NewEnd", "NewStart"]] = -1


#     scdf = scdf[~((scdf.NewStart == -1) | (scdf.NewEnd == -1))]
#     scdf = scdf.drop(["Start", "End"], axis=1)
#     scdf.rename(columns={"NewStart": "Start", "NewEnd": "End"}, inplace=True)

#     remove_mask = scdf.Start >= scdf.End

#     return scdf[~remove_mask]


def _subtraction_python(scdf, ocdf):
    """Subtract intervals without using coordinate values as internal sentinels."""
    if ocdf is None or len(ocdf) == 0 or scdf.empty:
        return scdf

    exclusions = (
        ocdf[["Start", "End"]]
        .dropna()
        .sort_values(["Start", "End"])
        .itertuples(index=False, name=None)
    )
    exclusions = [(int(start), int(end)) for start, end in exclusions if start < end]

    piece_positions = []
    piece_starts = []
    piece_ends = []

    for position, row in enumerate(scdf.itertuples(index=False)):
        start = int(row.Start)
        end = int(row.End)
        pieces = [(start, end)]

        for exclusion_start, exclusion_end in exclusions:
            next_pieces = []
            for piece_start, piece_end in pieces:
                if exclusion_end <= piece_start or exclusion_start >= piece_end:
                    next_pieces.append((piece_start, piece_end))
                    continue

                if exclusion_start > piece_start:
                    next_pieces.append((piece_start, min(exclusion_start, piece_end)))
                if exclusion_end < piece_end:
                    next_pieces.append((max(exclusion_end, piece_start), piece_end))

            pieces = next_pieces

        for piece_start, piece_end in pieces:
            if piece_start < piece_end:
                piece_positions.append(position)
                piece_starts.append(piece_start)
                piece_ends.append(piece_end)

    if not piece_positions:
        return scdf.iloc[0:0].copy()

    result = scdf.iloc[piece_positions].copy()
    result.index = pd.RangeIndex(len(result))
    result["Start"] = np.asarray(piece_starts, dtype=scdf.Start.dtype)
    result["End"] = np.asarray(piece_ends, dtype=scdf.End.dtype)
    return result


def _subtraction(scdf, ocdf, **kwargs):
    if ocdf.empty or scdf.empty:
        return scdf

    # The NCLS helper and the historical result filtering use -1 as a sentinel.
    # That is ambiguous for odometer/genomic coordinates where negative values
    # and literal -1 are valid business data.
    if (scdf["Start"] < 0).any() or (scdf["End"] < 0).any():
        return _subtraction_python(scdf, ocdf)

    strandedness = kwargs["strandedness"]
    strand = True if strandedness else False

    chromosome = scdf.Chromosome.head(1).iloc[0]
    kwargs["chromosome"] = chromosome

    if "Strand" in ocdf and strand:
        strand = scdf.Strand.head(1).iloc[0]
        kwargs["strand"] = strand

    o = NCLS(ocdf.Start.values, ocdf.End.values, ocdf.index.values)

    idx_self, new_starts, new_ends = o.set_difference_helper(
        scdf.Start.values, scdf.End.values, scdf.index.values, scdf.__num__.values
    )

    missing_idx = pd.Index(scdf.index).difference(idx_self)

    idx_to_drop = new_starts != -1

    new_starts = new_starts[idx_to_drop]
    new_ends = new_ends[idx_to_drop]

    idx_self = idx_self[idx_to_drop]
    new_starts = pd.Series(new_starts, index=idx_self)
    new_ends = pd.Series(new_ends, index=idx_self)

    scdf = scdf.reindex(missing_idx.union(idx_self)).sort_index()
    new_starts = new_starts.sort_index()
    new_ends = new_ends.sort_index()

    if len(idx_self):
        scdf.loc[scdf.index.isin(idx_self), "Start"] = new_starts.values
        scdf.loc[scdf.index.isin(idx_self), "End"] = new_ends.values

    if not scdf.empty:
        return scdf
    else:
        return None
