Calling CNVs with DECA
======================

DECA runs a three stage pipeline:

1.  Per-target coverage is computed from aligned reads from a single sample.
2.  The coverage from each sample is combined into a larger coverage matrix.
    This matrix is normalized using a PCA-based approach.
3.  A Hidden Markov Model (HMM) is used to discover copy number variants from
    the normalized coverage data.

DECA exposes various combinations of these stages in the DECA command line:

-  The ``coverage`` command runs only the first stage of the pipeline.
-  The ``normalize`` command runs only the second stage of the pipeline.
-  The ``discover`` command runs only the third stage of the pipeline.
-  The ``normalize_and_discover`` command runs the last two stages of the
   pipeline.
-  The `cnv` command runs the whole pipeline.

End-to-end copy number variant calling
--------------------------------------

We can call copy number variants from reads using DECA's ``cnv`` command. To do
this, we will need to pass three parameters:

-  ``-I``: The reads to analyze. The paths to each sample's reads should be
   separated by whitespace. Alternatively, a file with a list of paths can be
   passed instead with the ``-l`` flag.
-  ``-L``: The targeted regions to call copy number variants over. For an exome
   panel, this should be the file describing the regions covered by the exome
   capture kit (usually a BED or Interval_list file).
-  ``-o``: The path to write CNVs in GFF format.

Additionally, you may provide a second file which contains targets that should
not be analyzed. You can pass this file with the ``-exclude_targets`` parameter.
We also filter out targets that are too long/short (``-min_target_length`` and
``-max_target_length``), or that are too highly/insufficiently covered
(``-min_target_mean_RD`` and ``-max_target_mean_RD``), and samples whose
coverage appears to be poor/inconsistent (``-min_sample_mean_RD``,
``-max_sample_mean_RD``, and ``-max_sample_sd_RD``).

If saving the read depth or normalized Z-scores is desired, you can provide
paths to save these files with the ``-save_rd`` and ``-save_zscores`` flags.

Normalization Parameters
~~~~~~~~~~~~~~~~~~~~~~~~

Unlike XHMM, DECA avoids computation by only calculating the top principal
components. We compute enough principal components to be able to normalize out
a given amount of variance. To do this, we may need to run PCA multiple times.
Thus, there are latency implications to the initial number of principal
components we choose to compute. To override the default number of principal
components computed, pass the ``-initial_k_fraction`` parameter.
A fixed number of principal components can be removed by specifying the
`-fixed_pc_toremove` parameter.
