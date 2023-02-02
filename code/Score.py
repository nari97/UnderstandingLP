class Score:
    def __init__(self, hc, pca, selec):
        self.hc = hc
        self.pca = pca
        self.selec = selec

    def get(self):
        return self.hc, self.pca, self.selec

    def add(self, hc, pca, selec):
        self.hc += hc
        self.pca += pca
        self.selec += selec

    def divide(self, hc_d, pca_d, selec_d):
        self.hc = self.hc * 1.0 / (hc_d + 1e-6)
        self.pca = self.pca * 1.0 / (pca_d + 1e-6)
        self.selec = self.selec * 1.0 / (selec_d + 1e-6)