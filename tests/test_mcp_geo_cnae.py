import unittest
from typing import Any, Dict, List

from server import (
    buscar_coocorrencia_cnaes_global,
    buscar_coocorrencia_cnaes_municipio,
    buscar_dinamica_sobrevivencia_cnae,
    buscar_expansao_filiais_cnae,
    buscar_malha_municipal_vizinha,
    buscar_perfil_economico_municipio,
    buscar_perfil_economico_regiao_ibge,
    buscar_visao_macro_regiao,
)


class TestMCPGeoCNAEReal(unittest.TestCase):
    def test_buscar_coocorrencia_global_software(self) -> None:
        cnae = "6201-5/01"
        resultado: List[Dict[str, Any]] = buscar_coocorrencia_cnaes_global(cnae, limite=5)
        self.assertIsInstance(resultado, list)
        self.assertGreater(len(resultado), 0)

        # Valida que as frequências estão em ordem decrescente
        frequencias = [item["frequencia"] for item in resultado]
        self.assertEqual(frequencias, sorted(frequencias, reverse=True))

        # O CNAE com maior coocorrência nacional com desenvolvimento de software é 6204-0/00 (Consultoria em TI)
        self.assertEqual(resultado[0]["codigo"], "6204-0/00")
        self.assertGreater(resultado[0]["frequencia"], 30000)

    def test_buscar_coocorrencia_municipio_sao_paulo(self) -> None:
        cnae_sem_ponto = "6201501"
        ibge_sp = "3550308"
        resultado: List[Dict[str, Any]] = buscar_coocorrencia_cnaes_municipio(
            cnae_sem_ponto, ibge_sp, limite=5
        )
        self.assertIsInstance(resultado, list)
        self.assertGreater(len(resultado), 0)

        for item in resultado:
            self.assertEqual(item["codigo_ibge"], ibge_sp)

        # Frequência decrescente
        frequencias = [item["frequencia"] for item in resultado]
        self.assertEqual(frequencias, sorted(frequencias, reverse=True))

    def test_buscar_malha_municipal_curitiba(self) -> None:
        # Busca por nome do município de forma flexível e valida hierarquia regional
        resultado: Dict[str, Any] = buscar_malha_municipal_vizinha("Curitiba")

        self.assertTrue(resultado["encontrado"])
        self.assertEqual(resultado["codigo_ibge"], "4106902")
        self.assertEqual(resultado["nome"], "Curitiba")
        self.assertEqual(resultado["estado"]["uf"], "PR")
        self.assertEqual(resultado["estado"]["regiao"], "Sul")
        self.assertEqual(resultado["regiao_imediata_ibge"], "Curitiba")
        self.assertEqual(resultado["regiao_intermediaria_ibge"], "Curitiba")

        vizinhos = resultado["municipios_vizinhos"]
        self.assertGreater(len(vizinhos), 0)

        # Pinhais e São José dos Pinhais devem estar entre os vizinhos
        nomes_vizinhos = [v["nome"] for v in vizinhos]
        self.assertIn("Pinhais", nomes_vizinhos)
        self.assertIn("São José dos Pinhais", nomes_vizinhos)

        # Valida distância geodésica positiva
        for v in vizinhos:
            self.assertGreater(v["distancia_km"], 0.0)

    def test_buscar_perfil_economico_municipio_com_sobrevivencia(self) -> None:
        resultado: Dict[str, Any] = buscar_perfil_economico_municipio("3550308", top_cnaes=5)
        self.assertTrue(resultado["encontrado"])
        self.assertEqual(resultado["nome"], "São Paulo")
        self.assertEqual(resultado["regiao"], "Sudeste")
        self.assertGreater(resultado["total_empresas_ativas_mapeadas"], 1000000)
        self.assertEqual(len(resultado["principais_cnaes"]), 5)

        # Valida que as métricas de sobrevivência, dinamismo e regimes (MEI/Simples) foram computadas
        primeiro_cnae = resultado["principais_cnaes"][0]
        self.assertIn("taxa_sobrevivencia_5_anos", primeiro_cnae)
        self.assertIn("taxa_sobrevivencia_percentual", primeiro_cnae)
        self.assertIn("estoque_mei", primeiro_cnae)
        self.assertIn("estoque_simples", primeiro_cnae)
        self.assertIn("taxa_mei", primeiro_cnae)
        self.assertGreaterEqual(primeiro_cnae["taxa_sobrevivencia_5_anos"], 0.0)

    def test_buscar_dinamica_sobrevivencia_cnae(self) -> None:
        resultado: Dict[str, Any] = buscar_dinamica_sobrevivencia_cnae(
            codigo_cnae="6201-5/01", codigo_ibge="3550308"
        )
        self.assertTrue(resultado["encontrado"])
        self.assertEqual(resultado["municipio"]["nome"], "São Paulo")
        self.assertEqual(resultado["municipio"]["regiao"], "Sudeste")
        self.assertEqual(resultado["cnae"]["codigo"], "6201-5/01")

        indicadores = resultado["indicadores_mercado"]
        self.assertGreater(indicadores["estoque_empresas_ativas"], 1000)
        self.assertGreater(indicadores["taxa_sobrevivencia_5_anos"], 0.0)
        self.assertIn("diagnostico_setorial", indicadores)
        self.assertIn("estoque_mei", indicadores)
        self.assertIn("estoque_simples", indicadores)
        self.assertIn("taxa_mei", indicadores)
        self.assertIn("perfil_predominante_porte", indicadores)

    def test_buscar_expansao_filiais_cnae(self) -> None:
        # Software desenvolvendo filiais de consultoria/customização
        resultado: List[Dict[str, Any]] = buscar_expansao_filiais_cnae("6201-5/01", limite=5)
        self.assertIsInstance(resultado, list)
        self.assertGreater(len(resultado), 0)

        # Valida ordenação por frequência de filiais geradas
        freqs = [item["frequencia_filiais"] for item in resultado]
        self.assertEqual(freqs, sorted(freqs, reverse=True))

    def test_buscar_visao_macro_regiao_sul(self) -> None:
        resultado: Dict[str, Any] = buscar_visao_macro_regiao("Sul")
        self.assertTrue(resultado["encontrado"])
        self.assertEqual(resultado["regiao"], "Sul")
        self.assertEqual(resultado["total_estados"], 3)  # PR, SC, RS

        ufs = [e["uf"] for e in resultado["estados"]]
        self.assertIn("PR", ufs)
        self.assertIn("SC", ufs)
        self.assertIn("RS", ufs)
        self.assertGreater(resultado["total_municipios_mapeados"], 50)

    def test_buscar_perfil_economico_regiao_ibge(self) -> None:
        resultado: Dict[str, Any] = buscar_perfil_economico_regiao_ibge("Curitiba", top_cnaes=5)
        self.assertTrue(resultado["encontrado"])
        self.assertEqual(resultado["regiao_nome"], "Curitiba")
        self.assertIn(resultado["tipo_regiao_ibge"], ["Região Imediata", "Região Intermediária"])
        self.assertEqual(resultado["uf"], "PR")
        self.assertGreater(resultado["total_municipios_integrantes"], 0)
        self.assertIsInstance(resultado["principais_cnaes_polo_regional"], list)

    def test_municipio_inexistente(self) -> None:
        ibge_fantasma = "9999999"
        resultado: Dict[str, Any] = buscar_malha_municipal_vizinha(ibge_fantasma)
        self.assertFalse(resultado["encontrado"])
        self.assertEqual(resultado["municipios_vizinhos"], [])


if __name__ == "__main__":
    unittest.main()
