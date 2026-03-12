"""Generate dummy ad banner images for the ad banner analytics app."""
from PIL import Image, ImageDraw, ImageFont
import os

OUTPUT_DIR = "/Users/kfukamori/ad-banner-analytics/banners"

# Banner definitions matching the SQL data
BANNERS = [
    # Campaign 1: 春の新生活
    {"file": "campaign1/banner_01.png", "size": (300, 250), "bg": "#FFFFFF", "accent": "#E53935",
     "headline": "新生活、始めよう。\n最大30%OFF", "cta": "今すぐチェック", "style": "photo"},
    {"file": "campaign1/banner_02.png", "size": (300, 250), "bg": "#F5F5F5", "accent": "#1E88E5",
     "headline": "シンプルに、新しく。\n春の家電フェア", "cta": "詳しく見る", "style": "minimal"},
    {"file": "campaign1/banner_03.png", "size": (728, 90), "bg": "#212121", "accent": "#FDD835",
     "headline": "今だけ！春の超特価セール", "cta": "特価を見る", "style": "bold"},
    {"file": "campaign1/banner_04.png", "size": (320, 50), "bg": "#E8F5E9", "accent": "#43A047",
     "headline": "ワクワクする新生活を応援", "cta": "詳細", "style": "illust"},
    # Campaign 2: サマーセール
    {"file": "campaign2/banner_05.png", "size": (300, 250), "bg": "#FCE4EC", "accent": "#E91E63",
     "headline": "SUMMER SALE\n最大50%OFF", "cta": "SHOP NOW", "style": "bold"},
    {"file": "campaign2/banner_06.png", "size": (300, 250), "bg": "#FFFFFF", "accent": "#90CAF9",
     "headline": "この夏のトレンドを\nお得に", "cta": "コレクションを見る", "style": "photo"},
    {"file": "campaign2/banner_07.png", "size": (336, 280), "bg": "#FFF3E0", "accent": "#FF6F00",
     "headline": "期間限定\nサマーコレクション", "cta": "今すぐ購入", "style": "bold"},
    # Campaign 3: スマートウォッチ
    {"file": "campaign3/banner_08.png", "size": (300, 250), "bg": "#000000", "accent": "#00BCD4",
     "headline": "The Next Smart.\n新次元のスマートウォッチ", "cta": "製品を見る", "style": "minimal"},
    {"file": "campaign3/banner_09.png", "size": (300, 250), "bg": "#FAFAFA", "accent": "#4CAF50",
     "headline": "あなたの健康を、\n腕の上に。", "cta": "詳しく見る", "style": "photo"},
    {"file": "campaign3/banner_10.png", "size": (728, 90), "bg": "#1A1A2E", "accent": "#FF5722",
     "headline": "バッテリー7日間 | 血中酸素 | GPS", "cta": "今すぐ予約", "style": "minimal"},
    {"file": "campaign3/banner_11.png", "size": (300, 250), "bg": "#0D0D0D", "accent": "#2196F3",
     "headline": "Watch the\nDifference", "cta": "動画を見る", "style": "photo"},
    # Campaign 4: 年末ギフト
    {"file": "campaign4/banner_12.png", "size": (300, 250), "bg": "#FBE9E7", "accent": "#BF360C",
     "headline": "大切な人に、\nおいしい贈り物を", "cta": "ギフトを探す", "style": "photo"},
    {"file": "campaign4/banner_13.png", "size": (300, 250), "bg": "#3E2723", "accent": "#FFD700",
     "headline": "プレミアム\nギフトコレクション", "cta": "コレクションを見る", "style": "minimal"},
    {"file": "campaign4/banner_14.png", "size": (336, 280), "bg": "#FFEBEE", "accent": "#D32F2F",
     "headline": "冬のご褒美\nギフト特集", "cta": "今すぐ注文", "style": "illust"},
    # Campaign 5: 化粧品
    {"file": "campaign5/banner_15.png", "size": (300, 250), "bg": "#F8F0F0", "accent": "#C2185B",
     "headline": "New Me,\nNew Beauty.", "cta": "ブランドを体験", "style": "minimal"},
    {"file": "campaign5/banner_16.png", "size": (300, 250), "bg": "#E8F5E9", "accent": "#2E7D32",
     "headline": "自然由来成分93%\n新スキンケアライン", "cta": "成分を見る", "style": "photo"},
    {"file": "campaign5/banner_17.png", "size": (728, 90), "bg": "#FFFFFF", "accent": "#9C27B0",
     "headline": "14日間で実感。透明感のある素肌へ", "cta": "無料サンプル", "style": "photo"},
    {"file": "campaign5/banner_18.png", "size": (300, 250), "bg": "#FFF8E1", "accent": "#F57C00",
     "headline": "人気美容家も愛用！\nリニューアルコスメ", "cta": "詳しく見る", "style": "photo"},
]


def hex_to_rgb(h):
    h = h.lstrip("#")
    return tuple(int(h[i:i+2], 16) for i in (0, 2, 4))


def contrasting_text_color(bg_hex):
    r, g, b = hex_to_rgb(bg_hex)
    luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
    return "#000000" if luminance > 0.5 else "#FFFFFF"


def draw_decorative_elements(draw, w, h, accent_rgb, style):
    """Add style-specific decorative elements."""
    if style == "bold":
        # Diagonal stripe
        for i in range(-h, w, 40):
            draw.line([(i, 0), (i + h, h)], fill=(*accent_rgb, 40), width=15)
    elif style == "minimal":
        # Thin accent line at top
        draw.rectangle([(0, 0), (w, 4)], fill=accent_rgb)
        # Small accent square
        draw.rectangle([(w - 40, h - 40), (w - 10, h - 10)], fill=(*accent_rgb, 80))
    elif style == "photo":
        # Gradient overlay simulation (horizontal bars with varying opacity)
        for y in range(h // 2, h):
            opacity = int(180 * (y - h // 2) / (h // 2))
            draw.rectangle([(0, y), (w, y + 1)], fill=(0, 0, 0, opacity))
    elif style == "illust":
        # Playful circles
        import random
        random.seed(hash(str(w) + str(h)))
        for _ in range(8):
            cx = random.randint(0, w)
            cy = random.randint(0, h)
            r = random.randint(15, 50)
            draw.ellipse([(cx - r, cy - r), (cx + r, cy + r)],
                         fill=(*accent_rgb, 30), outline=(*accent_rgb, 60))


def generate_banner(banner_def):
    w, h = banner_def["size"]
    bg_rgb = hex_to_rgb(banner_def["bg"])
    accent_rgb = hex_to_rgb(banner_def["accent"])
    text_color = contrasting_text_color(banner_def["bg"])
    text_rgb = hex_to_rgb(text_color)

    # Create RGBA image
    img = Image.new("RGBA", (w, h), (*bg_rgb, 255))
    draw = ImageDraw.Draw(img, "RGBA")

    # Decorative elements
    draw_decorative_elements(draw, w, h, accent_rgb, banner_def["style"])

    # Headline text
    headline = banner_def["headline"]
    is_wide = w / h > 3  # wide banner like 728x90
    font_size = 14 if is_wide else max(16, min(28, h // 8))

    try:
        font = ImageFont.truetype("/System/Library/Fonts/ヒラギノ角ゴシック W6.ttc", font_size)
        font_small = ImageFont.truetype("/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc", max(10, font_size - 6))
    except (OSError, IOError):
        try:
            font = ImageFont.truetype("/System/Library/Fonts/Hiragino Sans GB.ttc", font_size)
            font_small = ImageFont.truetype("/System/Library/Fonts/Hiragino Sans GB.ttc", max(10, font_size - 6))
        except (OSError, IOError):
            font = ImageFont.load_default()
            font_small = font

    # Position headline
    if is_wide:
        # Wide banner: text left-aligned, CTA on right
        text_x = 20
        text_y = h // 2 - font_size // 2
        draw.text((text_x, text_y), headline.replace("\n", "  "), fill=text_rgb, font=font)
        # CTA button on right
        cta_text = banner_def["cta"]
        cta_w = len(cta_text) * (font_size - 2) + 20
        cta_h = font_size + 12
        cta_x = w - cta_w - 20
        cta_y = (h - cta_h) // 2
        draw.rounded_rectangle([(cta_x, cta_y), (cta_x + cta_w, cta_y + cta_h)],
                                radius=4, fill=accent_rgb)
        cta_text_color = contrasting_text_color(banner_def["accent"])
        draw.text((cta_x + 10, cta_y + 4), cta_text,
                  fill=hex_to_rgb(cta_text_color), font=font_small)
    else:
        # Square/rect banner: centered text, CTA button at bottom
        lines = headline.split("\n")
        total_text_h = len(lines) * (font_size + 6)
        text_y = max(20, (h - total_text_h - 50) // 2)
        for line in lines:
            bbox = draw.textbbox((0, 0), line, font=font)
            line_w = bbox[2] - bbox[0]
            text_x = (w - line_w) // 2
            draw.text((text_x, text_y), line, fill=text_rgb, font=font)
            text_y += font_size + 6

        # CTA button
        cta_text = banner_def["cta"]
        cta_bbox = draw.textbbox((0, 0), cta_text, font=font_small)
        cta_tw = cta_bbox[2] - cta_bbox[0]
        cta_w = cta_tw + 30
        cta_h = (font_size - 4) + 16
        cta_x = (w - cta_w) // 2
        cta_y = min(text_y + 15, h - cta_h - 15)
        draw.rounded_rectangle([(cta_x, cta_y), (cta_x + cta_w, cta_y + cta_h)],
                                radius=6, fill=accent_rgb)
        cta_text_color = contrasting_text_color(banner_def["accent"])
        draw.text((cta_x + 15, cta_y + 6), cta_text,
                  fill=hex_to_rgb(cta_text_color), font=font_small)

    # Convert to RGB for PNG
    final = Image.new("RGB", (w, h), bg_rgb)
    final.paste(img, mask=img.split()[3])

    out_path = os.path.join(OUTPUT_DIR, banner_def["file"])
    final.save(out_path, "PNG")
    return out_path


if __name__ == "__main__":
    for b in BANNERS:
        path = generate_banner(b)
        print(f"Generated: {path}")
    print(f"\nDone: {len(BANNERS)} banners generated")
