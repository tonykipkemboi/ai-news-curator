use chrono::{Local, Datelike};
use dotenv::dotenv;
use feed_rs::parser;
use futures::future::join_all;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::env;
use std::path::Path;

const DB_PATH: &str = "seen_items.db";
const ARXIV_URL: &str = "http://export.arxiv.org/api/query?search_query=cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.MA&sortBy=submittedDate&sortOrder=descending&max_results=30";
const HN_TOP_STORIES: &str = "https://hacker-news.firebaseio.com/v0/topstories.json";
const HN_KEYWORDS: &[&str] = &[
    "ai", "llm", "gpt", "claude", "agent", "ml", "transformer", "rag", 
    "anthropic", "openai", "reasoning", "model", "neural", "deep learning",
    "inference", "cuda", "gpu", "mcp", "token"
];
const DEEP_DIVE_KEYWORDS: &[&str] = &["agent", "mcp", "memory", "context", "tool", "reasoning", "multi-agent"];

type GenericError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ScoutReport {
    source: String,
    report: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Item {
    title: String,
    url: String,
    source: String,
    summary: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    comments: Vec<String>,
    full_text: Option<String>,
}

impl Item {
    fn id(&self) -> String {
        format!("{:x}", md5::compute(&self.url))
    }
}

#[derive(Debug, Deserialize)]
struct HnStory {
    title: Option<String>,
    url: Option<String>,
    #[serde(rename = "id")]
    _id: i64,
    kids: Option<Vec<i64>>,
}

struct SeenStore {
    conn: Connection,
}

impl SeenStore {
    fn new(path: &Path) -> Result<Self, GenericError> {
        let conn = Connection::open(path)?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS seen (
                id TEXT PRIMARY KEY,
                url TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )",
            [],
        )?;
        Ok(SeenStore { conn })
    }

    fn is_seen(&self, id: &str) -> bool {
        let mut stmt = self.conn.prepare("SELECT 1 FROM seen WHERE id = ?").unwrap();
        stmt.exists(params![id]).unwrap_or(false)
    }

    fn mark_seen(&self, id: &str, url: &str) -> Result<(), GenericError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO seen (id, url) VALUES (?, ?)",
            params![id, url],
        )?;
        Ok(())
    }
}

async fn fetch_arxiv_full_text(client: &reqwest::Client, arxiv_url: &str) -> Option<String> {
    // Attempt 1: Native arXiv HTML support
    let html_url = arxiv_url.replace("/abs/", "/html/");
    if let Ok(resp) = client.get(&html_url)
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .send().await {
            if resp.status().is_success() {
                if let Ok(text) = resp.text().await {
                    return Some(clean_html(&text));
                }
            }
        }

    // Attempt 2: ar5iv.org fallback
    let ar5iv_url = arxiv_url.replace("arxiv.org", "ar5iv.org");
    if let Ok(resp) = client.get(&ar5iv_url)
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        .send().await {
            if resp.status().is_success() {
                if let Ok(text) = resp.text().await {
                    // Check if it's a redirect or empty
                    if !text.contains("403 Forbidden") && !text.contains("Redirecting") {
                         return Some(clean_html(&text));
                    }
                }
            }
        }

    None
}

fn clean_html(html: &str) -> String {
    let text = html
        .split("<body")
        .nth(1)
        .unwrap_or(html)
        .split("</body>")
        .next()
        .unwrap_or(html);

    let mut clean_text = String::new();
    let mut in_tag = false;
    for c in text.chars() {
        if c == '<' { in_tag = true; }
        else if c == '>' { in_tag = false; }
        else if !in_tag { clean_text.push(c); }
    }
    clean_text.chars().take(10000).collect()
}

async fn fetch_arxiv() -> Result<Vec<Item>, GenericError> {
    println!("Fetching arXiv papers...");
    let client = reqwest::Client::new();
    let resp = client.get(ARXIV_URL).send().await?.bytes().await?;
    let feed = parser::parse(&resp[..])?;
    
    let mut items = Vec::new();
    for e in feed.entries {
        let title = e.title.as_ref().map(|t| t.content.clone()).unwrap_or_default();
        let summary = e.summary.as_ref().map(|s| s.content.clone()).unwrap_or_default();
        let url = e.links.first().map(|l| l.href.clone()).unwrap_or_default();
        
        let should_deep_dive = DEEP_DIVE_KEYWORDS.iter().any(|&kw| 
            title.to_lowercase().contains(kw) || summary.to_lowercase().contains(kw)
        );

        let full_text = if should_deep_dive {
            println!("🔍 Deep Dive Triggered for: {}", title);
            fetch_arxiv_full_text(&client, &url).await
        } else {
            None
        };

        items.push(Item {
            title,
            url,
            source: "arxiv".to_string(),
            summary: summary.chars().take(400).collect(),
            comments: Vec::new(),
            full_text,
        });
    }
    
    Ok(items)
}

async fn fetch_hn_comments(client: &reqwest::Client, kids: &[i64]) -> Vec<String> {
    let tasks: Vec<_> = kids.iter().take(10).map(|&id| {
        let client = client.clone();
        tokio::spawn(async move {
            let url = format!("https://hacker-news.firebaseio.com/v0/item/{}.json", id);
            let resp = client.get(url).send().await.ok()?.json::<serde_json::Value>().await.ok()?;
            resp["text"].as_str().map(|t| t.to_string())
        })
    }).collect();

    join_all(tasks).await.into_iter()
        .filter_map(|r| r.ok())
        .flatten()
        .collect()
}

async fn fetch_hn_item(client: &reqwest::Client, id: i64) -> Option<Item> {
    let url = format!("https://hacker-news.firebaseio.com/v0/item/{}.json", id);
    let story = client.get(url).send().await.ok()?.json::<HnStory>().await.ok()?;
    
    let title = story.title?;
    let title_lower = title.to_lowercase();
    
    if HN_KEYWORDS.iter().any(|&kw| title_lower.contains(kw)) {
        let comments = if let Some(kids) = story.kids {
            fetch_hn_comments(client, &kids).await
        } else {
            Vec::new()
        };

        return Some(Item {
            title,
            url: story.url.unwrap_or_else(|| format!("https://news.ycombinator.com/item?id={}", id)),
            source: "hn".to_string(),
            summary: String::new(),
            comments,
            full_text: None,
        });
    }
    None
}

async fn fetch_hn() -> Result<Vec<Item>, GenericError> {
    println!("Fetching Hacker News posts...");
    let client = reqwest::Client::new();
    let ids: Vec<i64> = client.get(HN_TOP_STORIES).send().await?.json().await?;
    
    let tasks: Vec<_> = ids.into_iter().take(200).map(|id| {
        let client = client.clone();
        tokio::spawn(async move { fetch_hn_item(&client, id).await })
    }).collect();
    
    let results = join_all(tasks).await;
    let items = results.into_iter()
        .filter_map(|r| r.ok().flatten())
        .collect();
    
    Ok(items)
}

async fn call_gemini(prompt: &str) -> Result<String, GenericError> {
    let api_key = env::var("GEMINI_API_KEY").expect("GEMINI_API_KEY must be set");
    let url = format!("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={}", api_key);

    let body = serde_json::json!({
        "contents": [{
            "parts": [{ "text": prompt }]
        }]
    });

    let client = reqwest::Client::new();
    let resp = client.post(url).json(&body).send().await?.json::<serde_json::Value>().await?;
    
    let text = resp["candidates"][0]["content"]["parts"][0]["text"]
        .as_str()
        .ok_or("Field 'text' not found in Gemini response")?
        .to_string();
        
    Ok(text)
}

async fn scout_arxiv(items: Vec<Item>) -> Result<ScoutReport, GenericError> {
    if items.is_empty() {
        return Ok(ScoutReport { source: "arxiv".into(), report: "No new papers today.".into() });
    }

    let mut content = String::new();
    for item in items.iter().take(20) {
        content.push_str(&format!("Title: {}\nUrl: {}\nAbstract: {}\n", item.title, item.url, item.summary));
        if let Some(ref full) = item.full_text {
            content.push_str(&format!("--- FULL TEXT SNIPPET (First 10k chars) ---\n{}\n---\n", full));
        }
        content.push_str("\n");
    }

    let prompt = format!(
        "You are an expert Technical Scout and AI Researcher. Your role is to analyze these arXiv papers for breakthrough architectural changes and engineering significance.\n\n\
        For papers where 'FULL TEXT SNIPPET' is provided, you MUST perform a deep-dive. Do not just summarize the abstract. Instead:\n\
        - Identify the specific novel components of the architecture.\n\
        - Look for benchmark details, training hardware, or optimization 'tricks' mentioned in the body.\n\
        - Explain WHY this is significant for a Senior AI Engineer building production systems.\n\n\
        For papers with only an 'Abstract', provide a concise summary of the core value proposition.\n\n\
        Papers to analyze:\n{}",
        content
    );

    let report = call_gemini(&prompt).await?;
    Ok(ScoutReport { source: "arxiv".into(), report })
}

async fn scout_hn(items: Vec<Item>) -> Result<ScoutReport, GenericError> {
    if items.is_empty() {
        return Ok(ScoutReport { source: "hn".into(), report: "No HN discussions today.".into() });
    }

    let mut content = String::new();
    for item in items.iter().take(15) {
        content.push_str(&format!("Discussion: {}\nLink: {}\nComments:\n{}\n\n", 
            item.title, 
            item.url, 
            item.comments.join("\n---\n"))
        );
    }

    let prompt = format!(
        "You are a Technical Community Scout. Your role is to analyze these Hacker News discussions and their corresponding top comments for community sentiment, technical roadblocks, and potential tool-building opportunities.\n\n\
        The comments contain 'juicy details' from developers working on the coal-face. Focus on:\n\
        - **Pain Points**: What are people struggling with? (e.g., 'this library is too slow', 'I hate setting up X', 'there's no good tool for Y').\n\
        - **Market Gaps**: Are people asking for features that don't exist yet?\n\
        - **Technical Skepticism**: Hidden 'gotchas' mentioned by developers.\n\
        - **Better Alternatives**: Mentions of competing libraries or ways to improve existing ones.\n\n\
        Your goal is to find 'signal' for a developer looking to build the next big AI tool.\n\n\
        Discussions & Comments:\n{}",
        content
    );

    let report = call_gemini(&prompt).await?;
    Ok(ScoutReport { source: "hn".into(), report })
}

async fn concierge_summarize(scout_reports: Vec<ScoutReport>, metadata: String) -> Result<String, GenericError> {
    let mut combined_reports = String::new();
    for r in scout_reports {
        combined_reports.push_str(&format!("--- REPORT FROM {} SCOUT ---\n{}\n\n", r.source.to_uppercase(), r.report));
    }

    let prompt = format!(
        "You are the Lead Technical Concierge for a Senior AI Engineer and Founder. Your goal is to synthesize the following scout reports into a standard, high-signal daily digest.\n\n\
        You MUST follow this Markdown structure EXACTLY. Do not add extra sections or remove these headers.\n\n\
        # AI Research & Engineering Digest\n\n\
        ## 1. Executive Summary (The Pulse)\n\
        [A 2-3 sentence overview of today's technical atmosphere.]\n\n\
        ## 2. 🔥 The 1% Signal (Must-Read)\n\
        [Select the single most important item. Provide a deeply technical 3-4 sentence explanation of its architectural impact.]\n\n\
        ## 3. 💡 Founder's Corner (Opportunities)\n\
        [Highlight 2-3 specific technical gaps or builder opportunities. Frame them as 'Problem' and 'Opportunity'.]\n\n\
        ## 4. 🛠️ Technical Intelligence (Deep Dives)\n\
        [Thematic groupings of the remaining items. Group by technology stack or architectural layer.]\n\n\
        ## 5. 📊 System Metadata\n\
        {}\n\n\
        Scout Reports for Synthesis:\n{}",
        metadata,
        combined_reports
    );

    call_gemini(&prompt).await
}

async fn send_digest_email(digest: &str, date_str: &str) -> Result<(), GenericError> {
    let api_key = match env::var("RESEND_API_KEY") {
        Ok(key) => key,
        Err(_) => {
            println!("⚠️ RESEND_API_KEY not set. Skipping email delivery.");
            return Ok(());
        }
    };
    let to_email = match env::var("DESTINATION_EMAIL") {
        Ok(email) => email,
        Err(_) => {
            println!("⚠️ DESTINATION_EMAIL not set. Skipping email delivery.");
            return Ok(());
        }
    };

    println!("📧 Rendering premium HTML digest...");
    let html_content = render_premium_email(digest);
    
    println!("📧 Sending digest email via Resend...");
    
    let url = "https://api.resend.com/emails";
    let body = serde_json::json!({
        "from": "AI Digest <onboarding@resend.dev>",
        "to": [to_email],
        "subject": format!("Daily AI News Digest - {}", date_str),
        "html": html_content,
        "text": digest,
    });

    let client = reqwest::Client::new();
    let resp = client.post(url)
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&body)
        .send()
        .await?;

    if resp.status().is_success() {
        println!("✅ Email sent successfully via Resend!");
    } else {
        let err_body = resp.text().await?;
        println!("❌ Failed to send email: {}", err_body);
    }

    Ok(())
}

fn render_premium_email(markdown: &str) -> String {
    use pulldown_cmark::{Parser, Options, html};

    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    let parser = Parser::new_ext(markdown, options);

    let mut html_output = String::new();
    html::push_html(&mut html_output, parser);

    format!(
        r#"<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <style>
        body {{
            background-color: #0f172a;
            color: #cbd5e1;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 40px 20px;
        }}
        .container {{
            max-width: 700px;
            margin: 0 auto;
            background-color: #1e293b;
            padding: 40px;
            border-radius: 12px;
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
            border: 1px solid #334155;
            color: #e2e8f0;
        }}
        h1 {{ color: #38bdf8; font-size: 28px; margin-top: 0; border-bottom: 1px solid #334155; padding-bottom: 10px; }}
        h2 {{ color: #f472b6; font-size: 22px; margin-top: 30px; border-left: 4px solid #f472b6; padding-left: 15px; }}
        h3 {{ color: #fbbf24; font-size: 18px; }}
        a {{ color: #38bdf8; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        p, li {{ color: #e2e8f0; margin-bottom: 12px; }}
        code {{ background-color: #334155; padding: 2px 5px; border-radius: 4px; font-family: monospace; color: #cbd5e1; }}
        hr {{ border: 0; border-top: 1px solid #334155; margin: 30px 0; }}
        .footer {{ font-size: 12px; color: #64748b; margin-top: 40px; text-align: center; }}
    </style>
</head>
<body>
    <div class="container">
        {}
        <div class="footer">
            Generated by your AI Research Agentic Fleet.
        </div>
    </div>
</body>
</html>"#,
        html_output
    )
}

fn save_raw_data(source: &str, items: &[Item]) -> Result<(), GenericError> {
    let now = Local::now();
    let path = format!(
        "data/raw/{}/{:02}/{:02}",
        now.year(),
        now.month(),
        now.day()
    );
    std::fs::create_dir_all(&path)?;
    
    let filename = format!("{}/{}_payload.json", path, source);
    let json = serde_json::to_string_pretty(items)?;
    std::fs::write(filename, json)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), GenericError> {
    dotenv().ok();
    let store = SeenStore::new(Path::new(DB_PATH))?;
    
    let arxiv_items = fetch_arxiv().await?;
    let hn_items = fetch_hn().await?;
    
    let new_arxiv: Vec<Item> = arxiv_items.into_iter()
        .filter(|i| !store.is_seen(&i.id()))
        .collect();
    let new_hn: Vec<Item> = hn_items.into_iter()
        .filter(|i| !store.is_seen(&i.id()))
        .collect();
        
    println!("Filtered to {} new arXiv items and {} new HN items", new_arxiv.len(), new_hn.len());
    
    if new_arxiv.is_empty() && new_hn.is_empty() {
        println!("No new items today.");
        return Ok(());
    }

    // Save Raw Data (Scalable Retention)
    save_raw_data("arxiv", &new_arxiv)?;
    save_raw_data("hn", &new_hn)?;
    println!("📦 Raw data archived to data/raw/");

    // Parallel Scouting
    let arxiv_items_to_scout = new_arxiv.clone();
    let hn_items_to_scout = new_hn.clone();
    
    let arxiv_task = tokio::spawn(async move { scout_arxiv(arxiv_items_to_scout).await });
    let hn_task = tokio::spawn(async move { scout_hn(hn_items_to_scout).await });

    let results = join_all(vec![arxiv_task, hn_task]).await;
    let scout_reports: Vec<ScoutReport> = results.into_iter()
        .filter_map(|r| r.ok())
        .filter_map(|r| r.ok())
        .collect();

    // Collect Metadata
    let metadata = format!(
        "- **Date**: {}\n- **arXiv Scout**: Processed {} papers\n- **HN Scout**: Scanned 200 stories, Extracted {} AI discussions with Top 10 comments",
        Local::now().format("%Y-%m-%d %H:%M:%S"),
        new_arxiv.len(),
        new_hn.len()
    );

    // Synthesis
    let digest = concierge_summarize(scout_reports, metadata).await?;
    println!("\n🗞️  AI MULTI-AGENT DIGEST\n=========================\n\n{}", digest);
    
    // File Saving
    let date_str = Local::now().format("%Y-%m-%d").to_string();
    let filename = format!("reports/AI_Digest_{}.md", date_str);
    std::fs::create_dir_all("reports")?;
    std::fs::write(&filename, &digest)?;
    println!("\n💾 Report saved to: {}", filename);
    
    // Email Delivery
    send_digest_email(&digest, &date_str).await?;
    
    // Mark as seen
    for item in new_arxiv {
        store.mark_seen(&item.id(), &item.url)?;
    }
    for item in new_hn {
        store.mark_seen(&item.id(), &item.url)?;
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_item_id_generation() {
        let item = Item {
            title: "Test Title".to_string(),
            url: "https://example.com/test".to_string(),
            source: "arxiv".to_string(),
            summary: "Summary".to_string(),
            comments: vec![],
            full_text: None,
        };
        let id1 = item.id();
        let id2 = item.id();
        assert_eq!(id1, id2, "ID should be deterministic");
        assert!(!id1.is_empty(), "ID should not be empty");
    }

    #[test]
    fn test_render_premium_email() {
        let markdown = "# Title\n* List item";
        let html = render_premium_email(markdown);
        assert!(html.contains("<!DOCTYPE html>"), "Should contain DOCTYPE");
        assert!(html.contains("<h1>Title</h1>"), "Should contain rendered header");
        assert!(html.contains("<li>List item</li>"), "Should contain rendered list");
        assert!(html.contains(".container"), "Should contain CSS styles");
    }

    #[test]
    fn test_seen_store_memory() -> Result<(), GenericError> {
        // Use in-memory DB for testing
        let store = SeenStore::new(Path::new(":memory:"))?;
        
        // Test 1: Initially not seen
        let id = "test_id_123";
        assert!(!store.is_seen(id), "Should not be seen initially");

        // Test 2: Mark as seen
        store.mark_seen(id, "http://example.com")?;
        assert!(store.is_seen(id), "Should be seen after marking");

        // Test 3: Double mark (should not error due to INSERT OR IGNORE)
        store.mark_seen(id, "http://example.com")?;
        assert!(store.is_seen(id), "Should still be seen");

        Ok(())
    }
}

    #[test]
    fn test_item_serialization_optimization() {
        let item_no_comments = Item {
            title: "ArXiv Paper".to_string(),
            url: "https://arxiv.org/abs/2101.00000".to_string(),
            source: "arxiv".to_string(),
            summary: "Abstract".to_string(),
            comments: vec![],
            full_text: None,
        };
        let json_no_comments = serde_json::to_string(&item_no_comments).unwrap();
        assert!(!json_no_comments.contains("comments"), "JSON should not contain comments field when empty");

        let item_with_comments = Item {
            title: "HN Story".to_string(),
            url: "https://news.ycombinator.com/item?id=123".to_string(),
            source: "hn".to_string(),
            summary: "".to_string(),
            comments: vec!["Great insight!".to_string()],
            full_text: None,
        };
        let json_with_comments = serde_json::to_string(&item_with_comments).unwrap();
        assert!(json_with_comments.contains("comments"), "JSON should contain comments field when populated");
    }
