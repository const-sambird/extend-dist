# Distributed Extend Index Selection

This repository is an implementation of 'Unlocking the Power of Diversity in Index Tuning for Cluster Databases' [1], a divergent design index tuning algorithm. That paper uses Extend [2] to generate index configurations for individual database replicas.

The implementation of Extend is (heavily) based on the one provided by the authors in [this repository](https://github.com/hyrise/index_selection_evaluation) for the paper *'Magic mirror in my hand, which is the best in the land? An Experimental Evaluation of Index Selection Algorithms'* [3], which is (c) 2020 Hasso-Plattner-Institut and used under the MIT Licence.

If you enjoyed this divergent index selection algorithm, why not try [qDINA](https://github.com/const-sambird/dina)?

## References

[1] H. Hang, X. Tang, B. Zhou, and J. Sun, “Unlocking the power of diversity in index tuning for cluster databases,” in Database and Expert Systems Applications, C. Strauss, T. Amagasa, G. Manco, G. Kotsis, A. M. Tjoa, and I. Khalil, Eds. Cham: Springer Nature Switzerland, 2024, pp. 185–200.

[2] R. Schlosser, J. Kossmann, and M. Boissier, “Efficient scalable multi-attribute index selection using recursive strategies,” in 35th IEEE International Conference on Data Engineering, ICDE 2019, Macao, China, April 8-11, 2019, 2019, pp. 1238–1249. [Online]. Available: https://doi.org/10.1109/ICDE.2019.00113.

[3] J. Kossmann, S. Halfpap, M. Jankrift, and R. Schlosser, “Magic mirror in my hand, which is the best in the land? an experimental evaluation of index selection algorithms,” Proc. VLDB Endow., vol. 13, no. 12, p. 2382–2395, Jul. 2020. [Online]. Available: https://doi-org.ezproxy.lib.ou.edu/10.14778/3407790.3407832.
