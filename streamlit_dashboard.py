import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import snowflake.connector

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Financier - Analyse Technique",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Titre principal
st.title("📈 Dashboard d'Analyse Technique Financière")
st.markdown("**Projet Big Data** - Visualisation des indicateurs techniques calculés avec Spark SQL")
st.markdown("---")

# 1. CHARGEMENT DES DONNÉES
@st.cache_data(ttl=3600)  # Cache pour 1 heure
def load_data():
    """
    Charge les données depuis Snowflake (colonnes en MAJUSCULES)
    """
    try:
        # Option 1: Connexion à Snowflake
        conn = snowflake.connector.connect(
            user='SOUKAINA',
            password='soso123456SOSO123456@',
            account='eqqseml-wv78446',
            warehouse='COMPUTE_WH',
            database='FINANCE_DB',
            schema='SOUKAINA_SCHEMA'
        )
        
        query = """
        SELECT 
            SYMBOLE,
            DATE_TRADING,
            PRIX_OUVERTURE,
            PRIX_HAUT,
            PRIX_BAS,
            PRIX_FERMETURE,
            VOLUME,
            MOYENNE_MOBILE_20J,
            MOYENNE_MOBILE_50J,
            MOYENNE_MOBILE_200J,
            RSI_14,
            MACD_LINE,
            MACD_SIGNAL,
            BOLLINGER_MIDDLE,
            BOLLINGER_UPPER,
            BOLLINGER_LOWER,
            BOLLINGER_PERCENT,
            TENDANCE_GLOBALE,
            NIVEAU_RSI,
            SIGNAL_TRADING,
            HORODATAGE_CALCUL
        FROM PROCESSED_DATA
        ORDER BY SYMBOLE, DATE_TRADING
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        # Renommer les colonnes en minuscules pour faciliter le code
        df = df.rename(columns=lambda x: x.lower())
        
        # Conversion des dates
        df['date_trading'] = pd.to_datetime(df['date_trading'])
        
        st.sidebar.success(f"✅ {len(df)} lignes chargées depuis Snowflake")
        
    except Exception as e:
        st.error(f"❌ Erreur Snowflake : {str(e)[:200]}")
        # Retourner un DataFrame vide pour éviter l'erreur
        df = pd.DataFrame()
    
    return df

# Barre latérale
st.sidebar.header("🔧 Paramètres de Visualisation")

# Charger les données
with st.spinner('Chargement des données depuis Snowflake...'):
    df = load_data()

# Vérifier si les données sont chargées
if df.empty:
    st.error("Aucune donnée chargée. Vérifiez la connexion Snowflake.")
    st.stop()

# Filtres dans la sidebar
st.sidebar.subheader("Filtres")

# Sélection du symbole
symboles = sorted(df['symbole'].unique())
symbole_selectionne = st.sidebar.selectbox(
    "Sélectionnez un symbole",
    symboles,
    index=0 if 'AAPL' in symboles else 0
)

# Sélection de la période (PERSONNALISABLE)
st.sidebar.subheader("Période d'analyse")
periode_options = {
    "1 mois": 30,
    "3 mois": 90,
    "6 mois": 180,
    "1 an": 365,
    "Personnalisée": "custom",
    "Toutes les données": None
}

periode_selectionnee = st.sidebar.selectbox(
    "Période",
    list(periode_options.keys())
)

# Si période personnalisée
if periode_options[periode_selectionnee] == "custom":
    col1, col2 = st.sidebar.columns(2)
    with col1:
        date_debut = st.date_input("Date début", value=df['date_trading'].min().date())
    with col2:
        date_fin = st.date_input("Date fin", value=df['date_trading'].max().date())
else:
    date_debut = None
    date_fin = None

# Appliquer les filtres
df_filtre = df[df['symbole'] == symbole_selectionne].copy()

if periode_selectionnee == "Personnalisée":
    df_filtre = df_filtre[
        (df_filtre['date_trading'] >= pd.Timestamp(date_debut)) &
        (df_filtre['date_trading'] <= pd.Timestamp(date_fin))
    ]
elif periode_options[periode_selectionnee] is not None:
    date_limite = datetime.now() - timedelta(days=periode_options[periode_selectionnee])
    df_filtre = df_filtre[df_filtre['date_trading'] >= date_limite]

# Afficher les statistiques de base
st.sidebar.subheader("📊 Statistiques")
st.sidebar.metric("Lignes chargées", f"{len(df):,}")
st.sidebar.metric("Symboles disponibles", len(symboles))
st.sidebar.metric("Période couverte", f"{df['date_trading'].min().date()} au {df['date_trading'].max().date()}")

# 2. VISUALISATION 1: CANDLESTICK + BANDES BOLLINGER
st.header("📈 1. Graphique Candlestick avec Bandes de Bollinger")

tab1, tab2, tab3 = st.tabs(["Graphique", "Données", "Description"])

with tab1:
    if not df_filtre.empty:
        fig_candlestick = go.Figure()
        
        # Ajouter les bandes de Bollinger
        fig_candlestick.add_trace(go.Scatter(
            x=df_filtre['date_trading'],
            y=df_filtre['bollinger_upper'],
            name='Bande Supérieure',
            line=dict(color='rgba(255, 99, 71, 0.7)', width=1),
            fill=None
        ))
        
        fig_candlestick.add_trace(go.Scatter(
            x=df_filtre['date_trading'],
            y=df_filtre['bollinger_lower'],
            name='Bande Inférieure',
            line=dict(color='rgba(30, 144, 255, 0.7)', width=1),
            fillcolor='rgba(173, 216, 230, 0.3)',
            fill='tonexty'
        ))
        
        fig_candlestick.add_trace(go.Scatter(
            x=df_filtre['date_trading'],
            y=df_filtre['bollinger_middle'],
            name='Moyenne Mobile 20j',
            line=dict(color='rgba(50, 205, 50, 0.9)', width=2)
        ))
        
        # Ajouter les chandeliers
        fig_candlestick.add_trace(go.Candlestick(
            x=df_filtre['date_trading'],
            open=df_filtre['prix_ouverture'],
            high=df_filtre['prix_haut'],
            low=df_filtre['prix_bas'],
            close=df_filtre['prix_fermeture'],
            name='Candlesticks',
            increasing_line_color='green',
            decreasing_line_color='red'
        ))
        
        fig_candlestick.update_layout(
            title=f'{symbole_selectionne} - Prix avec Bandes de Bollinger',
            yaxis_title='Prix ($)',
            xaxis_title='Date',
            template='plotly_white',
            hovermode='x unified',
            height=500,
            showlegend=True
        )
        
        fig_candlestick.update_xaxes(
            rangeslider_visible=False,
            rangeselector=dict(
                buttons=list([
                    dict(count=7, label="1w", step="day", stepmode="backward"),
                    dict(count=1, label="1m", step="month", stepmode="backward"),
                    dict(count=3, label="3m", step="month", stepmode="backward"),
                    dict(count=6, label="6m", step="month", stepmode="backward"),
                    dict(step="all")
                ])
            )
        )
        
        st.plotly_chart(fig_candlestick, use_container_width=True)
    else:
        st.warning("Aucune donnée disponible pour ce symbole/période")

with tab2:
    if not df_filtre.empty:
        st.dataframe(df_filtre[['date_trading', 'prix_ouverture', 'prix_haut', 'prix_bas', 
                               'prix_fermeture', 'bollinger_upper', 'bollinger_middle', 
                               'bollinger_lower']].tail(20), use_container_width=True)
    else:
        st.info("Aucune donnée à afficher")

with tab3:
    st.markdown("""
    **Description :**
    - **Candlesticks** : Représentation des prix d'ouverture, clôture, haut et bas
    - **Bandes de Bollinger** : Moyenne mobile 20 jours ± 2 écarts-types
    - **Zone bleue** : Zone entre les bandes (volatilité normale)
    """)

# 3. VISUALISATION 2: INDICATEURS RSI & MACD
st.header("📊 2. Indicateurs Techniques RSI & MACD")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Relative Strength Index (RSI 14)")
    
    if not df_filtre.empty:
        fig_rsi = go.Figure()
        fig_rsi.add_trace(go.Scatter(
            x=df_filtre['date_trading'],
            y=df_filtre['rsi_14'],
            name='RSI 14',
            line=dict(color='purple', width=2),
            fill='tozeroy',
            fillcolor='rgba(128, 0, 128, 0.1)'
        ))
        
        # Ajouter les lignes de surachat/survente
        fig_rsi.add_hline(y=70, line_dash="dash", line_color="red", 
                         annotation_text="Surachat (>70)", annotation_position="bottom right")
        fig_rsi.add_hline(y=30, line_dash="dash", line_color="green",
                         annotation_text="Survente (<30)", annotation_position="top right")
        
        fig_rsi.update_layout(
            height=300,
            yaxis_range=[0, 100],
            yaxis_title='RSI',
            template='plotly_white',
            showlegend=False
        )
        
        st.plotly_chart(fig_rsi, use_container_width=True)
        
        # Afficher le niveau RSI actuel
        derniere_valeur_rsi = df_filtre['rsi_14'].iloc[-1] if len(df_filtre) > 0 else 0
        niveau_rsi = df_filtre['niveau_rsi'].iloc[-1] if len(df_filtre) > 0 else "N/A"
        
        st.metric("Dernier RSI", f"{derniere_valeur_rsi:.2f}", niveau_rsi)
    else:
        st.info("Données RSI non disponibles")

with col2:
    st.subheader("Moving Average Convergence Divergence (MACD)")
    
    if not df_filtre.empty:
        fig_macd = go.Figure()
        fig_macd.add_trace(go.Scatter(
            x=df_filtre['date_trading'],
            y=df_filtre['macd_line'],
            name='Ligne MACD',
            line=dict(color='blue', width=2)
        ))
        fig_macd.add_trace(go.Scatter(
            x=df_filtre['date_trading'],
            y=df_filtre['macd_signal'],
            name='Ligne de Signal',
            line=dict(color='orange', width=2)
        ))
        
        # Ajouter les histogrammes MACD
        histogramme = df_filtre['macd_line'] - df_filtre['macd_signal']
        colors = ['green' if val >= 0 else 'red' for val in histogramme]
        
        fig_macd.add_trace(go.Bar(
            x=df_filtre['date_trading'],
            y=histogramme,
            name='Histogramme MACD',
            marker_color=colors,
            opacity=0.3
        ))
        
        fig_macd.update_layout(
            height=300,
            yaxis_title='MACD',
            template='plotly_white',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        
        st.plotly_chart(fig_macd, use_container_width=True)
        
        # Dernier signal MACD
        derniere_ligne = df_filtre['macd_line'].iloc[-1] if len(df_filtre) > 0 else 0
        derniere_signal = df_filtre['macd_signal'].iloc[-1] if len(df_filtre) > 0 else 0
        difference = derniere_ligne - derniere_signal
        
        if derniere_ligne > derniere_signal:
            signal = "📈 Hausse (MACD > Signal)"
            delta = f"+{difference:.4f}"
        else:
            signal = "📉 Baisse (MACD < Signal)"
            delta = f"{difference:.4f}"
        
        st.metric("Signal MACD", signal, delta)
    else:
        st.info("Données MACD non disponibles")

# 4. VISUALISATION 3: MATRICE DE CORRÉLATIONS (HEATMAP)
st.header("🔥 3. Matrice de Corrélations entre Symboles")

if len(df) > 0:
    # Préparer les données pour la corrélation
    df_corr = df.pivot_table(
        values='prix_fermeture',
        index='date_trading',
        columns='symbole'
    ).corr().round(2)
    
    fig_heatmap = px.imshow(
        df_corr,
        text_auto=True,
        aspect="auto",
        color_continuous_scale='RdBu',
        title='Matrice de Corrélation entre les Prix de Clôture',
        labels=dict(color="Coefficient de Corrélation")
    )
    
    fig_heatmap.update_layout(
        height=500,
        xaxis_title="Symbole",
        yaxis_title="Symbole"
    )
    
    st.plotly_chart(fig_heatmap, use_container_width=True)
    
    # Options pour la heatmap
    col_a, col_b = st.columns(2)
    with col_a:
        show_values = st.checkbox("Afficher les valeurs", value=True)
    with col_b:
        color_scale = st.selectbox(
            "Échelle de couleurs",
            ['RdBu', 'Viridis', 'Plasma', 'Inferno', 'Magma']
        )
    
    if show_values:
        fig_heatmap.update_traces(texttemplate='%{text}', textfont={"size": 10})
    else:
        fig_heatmap.update_traces(texttemplate='')
else:
    st.info("Pas assez de données pour la matrice de corrélation")

# 5. VISUALISATION 4: PERFORMANCE PAR PÉRIODE (PERSONNALISABLE)
st.header("📅 4. Performance par Période")

# Options de personnalisation
col_period1, col_period2, col_period3 = st.columns(3)

with col_period1:
    granularite = st.selectbox(
        "Granularité temporelle",
        ["Mensuelle", "Trimestrielle", "Semestrielle", "Annuelle"],
        key="granularite"
    )

with col_period2:
    metrique = st.selectbox(
        "Métrique à analyser",
        ["Prix moyen", "Volume total", "Signaux d'achat", "Volatilité"],
        key="metrique"
    )

with col_period3:
    symboles_comparaison = st.multiselect(
        "Symboles à comparer",
        symboles,
        default=[symbole_selectionne] if symboles else [],
        key="comparaison"
    )

if symboles_comparaison and len(df) > 0:
    # Préparer les données selon la granularité
    df_perf = df[df['symbole'].isin(symboles_comparaison)].copy()
    
    if granularite == "Mensuelle":
        df_perf['periode'] = df_perf['date_trading'].dt.strftime('%Y-%m')
        periode_label = "Mois"
    elif granularite == "Trimestrielle":
        df_perf['periode'] = df_perf['date_trading'].dt.to_period('Q').astype(str)
        periode_label = "Trimestre"
    elif granularite == "Semestrielle":
        df_perf['annee'] = df_perf['date_trading'].dt.year
        df_perf['semestre'] = (df_perf['date_trading'].dt.month - 1) // 6 + 1
        df_perf['periode'] = df_perf['annee'].astype(str) + '-S' + df_perf['semestre'].astype(str)
        periode_label = "Semestre"
    else:  # Annuelle
        df_perf['periode'] = df_perf['date_trading'].dt.year.astype(str)
        periode_label = "Année"
    
    # Calculer les métriques
    if metrique == "Prix moyen":
        performance = df_perf.groupby(['symbole', 'periode']).agg({
            'prix_fermeture': 'mean',
            'prix_haut': 'max',
            'prix_bas': 'min'
        }).round(2).reset_index()
        
        fig_perf = go.Figure()
        for symb in symboles_comparaison:
            df_symb = performance[performance['symbole'] == symb]
            fig_perf.add_trace(go.Scatter(
                x=df_symb['periode'],
                y=df_symb['prix_fermeture'],
                name=symb,
                mode='lines+markers',
                line=dict(width=2)
            ))
        
        fig_perf.update_layout(
            title=f'Évolution du Prix Moyen par {periode_label}',
            yaxis_title='Prix Moyen ($)',
            xaxis_title=periode_label,
            height=400,
            template='plotly_white'
        )
    
    elif metrique == "Volume total":
        performance = df_perf.groupby(['symbole', 'periode']).agg({
            'volume': 'sum'
        }).reset_index()
        
        fig_perf = px.bar(
            performance,
            x='periode',
            y='volume',
            color='symbole',
            barmode='group',
            title=f'Volume Total par {periode_label}'
        )
        fig_perf.update_layout(height=400)
    
    elif metrique == "Signaux d'achat":
        performance = df_perf.groupby(['symbole', 'periode']).agg({
            'signal_trading': lambda x: (x == 'SIGNAL_ACHAT').sum()
        }).reset_index()
        performance.columns = ['symbole', 'periode', 'signaux_achat']
        
        fig_perf = px.bar(
            performance,
            x='periode',
            y='signaux_achat',
            color='symbole',
            barmode='group',
            title=f'Signaux d\'Achat par {periode_label}'
        )
        fig_perf.update_layout(height=400)
    
    else:  # Volatilité
        performance = df_perf.groupby(['symbole', 'periode']).agg({
            'prix_fermeture': lambda x: x.std() / x.mean() * 100
        }).reset_index()
        performance.columns = ['symbole', 'periode', 'volatilite_pourcent']
        
        fig_perf = px.line(
            performance,
            x='periode',
            y='volatilite_pourcent',
            color='symbole',
            markers=True,
            title=f'Volatilité (%) par {periode_label}'
        )
        fig_perf.update_layout(height=400)
    
    st.plotly_chart(fig_perf, use_container_width=True)
else:
    st.info("Sélectionnez des symboles pour voir la performance")

# 6. TABLEAU DE BORD DES SIGNALS
st.header("🎯 Tableau de Bord des Signaux de Trading")

if not df_filtre.empty:
    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    
    # Calculer les statistiques des signaux
    signaux_stats = df_filtre['signal_trading'].value_counts()
    total_signaux = signaux_stats.sum()
    total_signaux = total_signaux if total_signaux > 0 else 1
    
    with col_s1:
        signaux_achat = signaux_stats.get('SIGNAL_ACHAT', 0) + signaux_stats.get('POTENTIEL_ACHAT_BB', 0)
        st.metric("Signaux ACHAT", signaux_achat, f"{(signaux_achat/total_signaux*100):.1f}%")
    
    with col_s2:
        signaux_vente = signaux_stats.get('SIGNAL_VENTE', 0) + signaux_stats.get('POTENTIEL_VENTE_BB', 0)
        st.metric("Signaux VENTE", signaux_vente, f"{(signaux_vente/total_signaux*100):.1f}%")
    
    with col_s3:
        tendance_actuelle = df_filtre['tendance_globale'].iloc[-1] if len(df_filtre) > 0 else "N/A"
        st.metric("Tendance Actuelle", tendance_actuelle)
    
    with col_s4:
        dernier_signal = df_filtre['signal_trading'].iloc[-1] if len(df_filtre) > 0 else "N/A"
        st.metric("Dernier Signal", dernier_signal)
    
    # Tableau des derniers signaux
    st.subheader("Derniers Signaux")
    df_signaux = df_filtre[['date_trading', 'prix_fermeture', 'rsi_14', 'macd_line', 
                            'niveau_rsi', 'signal_trading', 'tendance_globale']].tail(10)
    st.dataframe(df_signaux, use_container_width=True)
else:
    st.info("Aucun signal disponible pour ce symbole")

# Pied de page
st.markdown("---")
st.markdown("**Dashboard réalisé avec :**")
col_f1, col_f2, col_f3, col_f4 = st.columns(4)
with col_f1:
    st.markdown("• **Apache Spark SQL**")
with col_f2:
    st.markdown("• **Snowflake**")
with col_f3:
    st.markdown("• **Streamlit**")
with col_f4:
    st.markdown("• **Plotly**")

st.caption(f"Dernière mise à jour: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")