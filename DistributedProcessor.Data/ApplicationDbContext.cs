using DistributedProcessor.Data.Models;
using Microsoft.EntityFrameworkCore;

namespace DistributedProcessor.Data
{
    public class ApplicationDbContext : DbContext
    {
        public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
            : base(options)
        {
        }

        // DbSets
        public DbSet<SourceData> SourceData { get; set; }
        public DbSet<CalculatedResult> CalculatedResults { get; set; }
        public DbSet<JobExecution> JobExecutions { get; set; }
        public DbSet<TaskLog> TaskLogs { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            // Configure SourceData
            modelBuilder.Entity<SourceData>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.HasIndex(e => new { e.Fund, e.Symbol, e.Date });
                entity.Property(e => e.OpenPrice).HasColumnType("decimal(18,6)");
                entity.Property(e => e.ClosePrice).HasColumnType("decimal(18,6)");
                entity.Property(e => e.High).HasColumnType("decimal(18,6)");
                entity.Property(e => e.Low).HasColumnType("decimal(18,6)");
            });

            // Configure CalculatedResult
            modelBuilder.Entity<CalculatedResult>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.HasIndex(e => e.TaskId);
                entity.HasIndex(e => e.JobId);
                entity.HasIndex(e => new { e.Fund, e.Symbol });
                entity.Property(e => e.Price).HasColumnType("decimal(18,6)");
                entity.Property(e => e.Returns).HasColumnType("decimal(18,10)");
                entity.Property(e => e.CumulativeReturns).HasColumnType("decimal(18,10)");
            });

            // Configure JobExecution
            modelBuilder.Entity<JobExecution>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.HasIndex(e => e.JobId).IsUnique();
                entity.HasIndex(e => e.Status);
                entity.HasIndex(e => e.Fund);
                entity.HasIndex(e => e.SubmittedAt);

                entity.Property(e => e.JobId).IsRequired().HasMaxLength(450);
                entity.Property(e => e.Fund).IsRequired().HasMaxLength(100);
                entity.Property(e => e.Status).IsRequired().HasMaxLength(50).HasDefaultValue("Pending");
                entity.Property(e => e.CreatedBy).HasMaxLength(100);
            });

            // Configure TaskLog
            modelBuilder.Entity<TaskLog>(entity =>
            {
                entity.HasKey(e => e.Id);
                entity.HasIndex(e => e.TaskId).IsUnique();
                entity.HasIndex(e => e.JobId);
                entity.HasIndex(e => e.Status);
                entity.HasIndex(e => new { e.Fund, e.Symbol });
                entity.HasIndex(e => e.WorkerId);

                entity.Property(e => e.TaskId).IsRequired().HasMaxLength(450);
                entity.Property(e => e.JobId).IsRequired().HasMaxLength(450);
                entity.Property(e => e.Fund).HasMaxLength(100);
                entity.Property(e => e.Symbol).HasMaxLength(100);
                entity.Property(e => e.Status).IsRequired().HasMaxLength(50).HasDefaultValue("Pending");
                entity.Property(e => e.WorkerId).HasMaxLength(255);

                // Configure foreign key relationship
                entity.HasOne(t => t.JobExecution)
                    .WithMany(j => j.TaskLogs)
                    .HasForeignKey(t => t.JobId)
                    .HasPrincipalKey(j => j.JobId)
                    .OnDelete(DeleteBehavior.Cascade);
            });
        }
    }
}
