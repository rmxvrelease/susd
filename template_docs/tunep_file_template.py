FILE_HEADER = '''
    \\documentclass{report}
    \\usepackage{longtable}
    \\usepackage[margin=1.0cm]{geometry}

    \\title{Laudo Quantitativo}
    \\author{Rafael Monteiro}
    \\date{\\today}


    \\begin{document}

    \\maketitle
'''

CONCLUSAO = '''
    \\section{Conclusões}
    TODO: Escrever conclusão
'''

TOTAL_HEADER = '''
\\section{Resumo Total}
    TODO: Escrever breve descrição
\\begin{longtable}[c]{|p{2.4cm}|p{2.6cm}|p{2.6cm}|p{2.3cm}|}
	\\caption{Resumo total} \\\\ \\hline
	\\textbf{Diferença TUNEP/IVR} &
	\\textbf{Atualização} &
	\\textbf{Total devido}
	\\endhead \\hline
'''

TOTAL_FOOTER = '''
    \\end{longtable}
'''

YEAR_HEADER = '''
\\section{Resumo Anual}
    TODO: Escrever breve descrição
\\begin{longtable}[c]{|p{1.7cm}|p{2.6cm}|p{2.6cm}|p{2.3cm}|}
	\\caption{Resumo ano a ano} \\\\ \\hline
	\\textbf{Ano} &
	\\textbf{Diferença TUNEP/IVR} &
	\\textbf{Atualização} &
	\\textbf{Total devido}
	\\endhead \\hline
'''

YEAR_FOOTER = '''
    \\end{longtable}
'''

MONTH_HEADER = '''
\\section{Resumo mensal}
    TODO: Escrever breve descrição

\\begin{longtable}[c]{|p{1.7cm}|p{2.6cm}|p{2.6cm}|p{2.3cm}|p{2.3cm}|c|}
	\\caption{Resumo mês a mês} \\\\ \\hline
	\\textbf{Mês} &
	\\textbf{Valor original pago pelo SUS} &
	\\textbf{Diferença TUNEP/IVR} &
	\\textbf{Atualização} &
	\\textbf{Total devido}
	\\endhead \\hline
'''

MONTH_FOOTER = '''
\\end{longtable}
'''

FILE_FOOTER =  '''\\end{document}'''
