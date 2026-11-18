@echo off
rem Caminho do diretorio atual do .bat
set "PROJECT_DIR=%~dp0"

rem Muda para esse diretorio
cd /d "%PROJECT_DIR%"

:: (opcional) ativar venv
call "%PROJECT_DIR%\venv\Scripts\activate.bat"

:: roda o arquivamento RCS
python -m main arquivar
