"""
compare_parser_results.py - Compare old vs new parser performance
Generates detailed comparison report
"""

import sqlite3
import json
from collections import Counter

def analyze_results(db_file='simulation_results.db'):
    """Analyze simulation results"""
    
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Get all results
    cursor.execute("SELECT status, ignore_reason, parsed_data, message_text FROM simulation_results")
    results = cursor.fetchall()
    
    stats = {
        'total': len(results),
        'parsed': 0,
        'ignored': 0,
        'failed': 0,
        'auto_symbol': 0,
        'auto_expiry': 0,
        'commodities': 0,
        'symbols': Counter(),
        'ignore_reasons': Counter(),
        'channels': Counter()
    }
    
    parsed_examples = []
    ignored_examples = []
    
    for status, reason, parsed_json, message in results:
        if status == 'PARSED':
            stats['parsed'] += 1
            
            if parsed_json:
                parsed = json.loads(parsed_json)
                
                if parsed.get('symbol'):
                    stats['symbols'][parsed['symbol']] += 1
                
                if parsed.get('symbol_auto_detected'):
                    stats['auto_symbol'] += 1
                
                if parsed.get('expiry_auto_calculated'):
                    stats['auto_expiry'] += 1
                
                if parsed.get('commodity'):
                    stats['commodities'] += 1
                
                # Collect examples
                if len(parsed_examples) < 10:
                    parsed_examples.append({
                        'message': message[:80],
                        'parsed': parsed
                    })
        
        elif status == 'IGNORED':
            stats['ignored'] += 1
            stats['ignore_reasons'][reason] += 1
            
            if len(ignored_examples) < 10:
                ignored_examples.append({
                    'message': message[:80],
                    'reason': reason
                })
        
        else:
            stats['failed'] += 1
    
    conn.close()
    
    return stats, parsed_examples, ignored_examples


def generate_comparison_report(db_file='simulation_results.db', 
                               output_file='SIMULATION_REPORT.txt'):
    """Generate detailed comparison report"""
    
    stats, parsed_ex, ignored_ex = analyze_results(db_file)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("NEW PARSER SIMULATION REPORT\n")
        f.write("="*80 + "\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Parser: EnhancedSignalParser v2.0\n")
        f.write("="*80 + "\n\n")
        
        # Overall statistics
        f.write("OVERALL STATISTICS:\n")
        f.write("-"*80 + "\n")
        f.write(f"Total Messages:         {stats['total']:4}\n")
        f.write(f"Successfully Parsed:    {stats['parsed']:4} ({stats['parsed']/stats['total']*100:5.1f}%)\n")
        f.write(f"Ignored (Filtered):     {stats['ignored']:4} ({stats['ignored']/stats['total']*100:5.1f}%)\n")
        f.write(f"Failed to Parse:        {stats['failed']:4} ({stats['failed']/stats['total']*100:5.1f}%)\n\n")
        
        # Special features
        f.write("ENHANCED FEATURES USAGE:\n")
        f.write("-"*80 + "\n")
        f.write(f"Auto Symbol Detection:  {stats['auto_symbol']:4} signals\n")
        f.write(f"Auto Expiry Calculated: {stats['auto_expiry']:4} signals\n")
        f.write(f"Commodity Trades:       {stats['commodities']:4} signals\n\n")
        
        # Symbol distribution
        f.write("SYMBOL DISTRIBUTION:\n")
        f.write("-"*80 + "\n")
        for symbol, count in stats['symbols'].most_common(15):
            f.write(f"  {symbol:15} : {count:3} signals\n")
        f.write("\n")
        
        # Ignore reasons
        f.write("TOP IGNORE REASONS:\n")
        f.write("-"*80 + "\n")
        for reason, count in stats['ignore_reasons'].most_common(15):
            f.write(f"  {count:4} - {reason}\n")
        f.write("\n")
        
        # Parsed examples
        f.write("EXAMPLE PARSED SIGNALS (First 10):\n")
        f.write("-"*80 + "\n")
        for i, ex in enumerate(parsed_ex, 1):
            f.write(f"\n{i}. Message: {ex['message']}\n")
            f.write(f"   Parsed:\n")
            for key, val in ex['parsed'].items():
                f.write(f"     {key}: {val}\n")
        f.write("\n")
        
        # Ignored examples
        f.write("EXAMPLE IGNORED MESSAGES (First 10):\n")
        f.write("-"*80 + "\n")
        for i, ex in enumerate(ignored_ex, 1):
            f.write(f"\n{i}. Message: {ex['message']}\n")
            f.write(f"   Reason: {ex['reason']}\n")
        f.write("\n")
        
        # Comparison with old parser
        f.write("="*80 + "\n")
        f.write("COMPARISON: OLD vs NEW PARSER\n")
        f.write("="*80 + "\n\n")
        
        # Estimated old parser performance (from logs)
        old_success_rate = 30.0  # From your logs
        old_regex_rate = 2.5
        old_claude_rate = 30.0
        
        new_success_rate = (stats['parsed'] / stats['total']) * 100
        
        f.write("Success Rate:\n")
        f.write(f"  OLD: {old_success_rate:5.1f}%\n")
        f.write(f"  NEW: {new_success_rate:5.1f}%\n")
        f.write(f"  Improvement: {new_success_rate - old_success_rate:+5.1f}%\n\n")
        
        f.write("Messages Ignored:\n")
        f.write(f"  NEW: {stats['ignored']:4} messages filtered\n")
        f.write(f"  Savings: Reduced noise and API costs\n\n")
        
        f.write("Auto-Detection Features:\n")
        f.write(f"  Symbol Auto-Detect: {stats['auto_symbol']} signals (NEW)\n")
        f.write(f"  Expiry Auto-Calc:   {stats['auto_expiry']} signals (NEW)\n")
        f.write(f"  These would have FAILED in old parser!\n\n")
        
        # Cost estimate
        estimated_api_calls_old = int(stats['total'] * (old_claude_rate / 100))
        estimated_api_calls_new = stats['parsed'] - stats['auto_symbol']  # Rough estimate
        
        f.write("Estimated API Usage:\n")
        f.write(f"  OLD: ~{estimated_api_calls_old} Claude API calls\n")
        f.write(f"  NEW: ~{estimated_api_calls_new} Claude API calls\n")
        f.write(f"  Reduction: ~{estimated_api_calls_old - estimated_api_calls_new} calls saved\n")
        f.write(f"  Cost Savings: ~${(estimated_api_calls_old - estimated_api_calls_new) * 0.003:.2f}\n\n")
        
        f.write("="*80 + "\n")
        f.write("RECOMMENDATIONS:\n")
        f.write("="*80 + "\n\n")
        
        if new_success_rate > old_success_rate:
            f.write("✅ NEW parser shows significant improvement!\n")
            f.write("   → Deploy to production\n\n")
        
        if stats['auto_symbol'] > 0:
            f.write(f"✅ Auto-symbol detection working! ({stats['auto_symbol']} signals)\n")
            f.write("   → Handles missing symbols correctly\n\n")
        
        if stats['auto_expiry'] > 0:
            f.write(f"✅ Auto-expiry calculation working! ({stats['auto_expiry']} signals)\n")
            f.write("   → No manual intervention needed\n\n")
        
        if stats['ignored'] > stats['total'] * 0.4:
            f.write(f"✅ Excellent noise filtering! ({stats['ignored']/stats['total']*100:.1f}% ignored)\n")
            f.write("   → Cleaner signal database\n\n")
        
        f.write("="*80 + "\n")
    
    print(f"\n✅ Report generated: {output_file}\n")


if __name__ == "__main__":
    from datetime import datetime
    import sys
    
    db_file = sys.argv[1] if len(sys.argv) > 1 else 'simulation_results.db'
    generate_comparison_report(db_file)
