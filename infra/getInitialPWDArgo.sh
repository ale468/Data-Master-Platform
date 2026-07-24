encoded=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}")
echo "$encoded" | base64 --decode
read -p " Pressione Enter para sair..."
